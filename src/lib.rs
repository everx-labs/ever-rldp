use adnl::{ 
    dump, 
    common::{deserialize, KeyId, QueryId, serialize, serialize_inplace, Subscriber, Version},
    node::AdnlNode
};
use rand::Rng;
use std::{
    cmp::min, sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}}, 
    time::{Duration, Instant}
};
use ton_api::{
    IntoBoxed, 
    ton::{
        self, fec::{Type as FecType, type_::RaptorQ as FecTypeRaptorQ}, 
        rldp::{
            Message as RldpMessageBoxed, MessagePart as RldpMessagePartBoxed, 
            message::Query as RldpQuery, messagepart::Complete as RldpComplete, 
            messagepart::Confirm as RldpConfirm, messagepart::MessagePart as RldpMessagePart
        }
    }
};
use ton_types::{fail, Result};
pub use raptorq;

const TARGET: &str = "rldp";

type TransferId = [u8; 32];

/// RaptorQ decoder
pub struct RaptorqDecoder {
    engine: raptorq::Decoder,
    params: FecTypeRaptorQ,
    seqno: u32
}

impl RaptorqDecoder {

    /// Construct with parameter
    pub fn with_params(params: FecTypeRaptorQ) -> Self {
        Self {
            engine: raptorq::Decoder::new(
                raptorq::ObjectTransmissionInformation::with_defaults(
                    params.data_size as u64, 
                    params.symbol_size as u16
                )
            ),
            params,
            seqno: 0
        }
    }

    /// Decode
    pub fn decode(&mut self, seqno: u32, data: &[u8]) -> Option<Vec<u8>> {
        let packet = raptorq::EncodingPacket::new(
            raptorq::PayloadId::new(0, seqno),
            data.to_vec()
        );
        self.seqno = seqno;
        self.engine.decode(packet)
    }

    /// Parameters
    pub fn params(&self) -> &FecTypeRaptorQ {
        &self.params
    }

}

struct RecvTransfer {
    buf: Vec<u8>,
    complete: RldpMessagePartBoxed,
    confirm: RldpMessagePartBoxed,
    confirm_count: usize,
    data: Vec<u8>,
    decoder: Option<RaptorqDecoder>,
    part: u32,
    total_size: Option<usize>
}

impl RecvTransfer {

    fn new(transfer_id: TransferId) -> Self {
        Self { 
            buf: Vec::new(),
            complete: RldpComplete {
                transfer_id: ton::int256(transfer_id.clone()),
                part: 0
            }.into_boxed(),
            confirm: RldpConfirm {
                transfer_id: ton::int256(transfer_id.clone()),
                part: 0,
                seqno: 0
            }.into_boxed(),
            confirm_count: 0,
            data: Vec::new(),
            decoder: None,
            part: 0,
            total_size: None
        }
    }

    fn complete(&mut self) -> Result<&mut RldpComplete> {
        match self.complete { 
            RldpMessagePartBoxed::Rldp_Complete(ref mut msg) => Ok(msg),
            _ => fail!("INTERNAL ERROR: RLDP complete message mismatch")
        }
    }

    fn confirm(&mut self) -> Result<&mut RldpConfirm> {
        match self.confirm { 
            RldpMessagePartBoxed::Rldp_Confirm(ref mut msg) => Ok(msg),
            _ => fail!("INTERNAL ERROR: RLDP confirm message mismatch")
        }
    }

    fn process_chunk(&mut self, message: Box<RldpMessagePart>) -> Result<Option<&[u8]>> {
        let fec_type = if let FecType::Fec_RaptorQ(fec_type) = message.fec_type {
            fec_type
        } else {
            fail!("Unsupported FEC type in RLDP packet")
        };
        let total_size = if let Some(total_size) = self.total_size {
            if total_size != message.total_size as usize {
                fail!("Incorrect total size in RLDP packet")
            }
            total_size
        } else {
            let total_size = message.total_size as usize;
            self.total_size = Some(total_size);
            self.data.reserve_exact(total_size);
            total_size
        };
        let decoder = if let Some(decoder) = &mut self.decoder {
            if self.part == message.part as u32 {
                if fec_type.as_ref() != &decoder.params {
                    fail!("Incorrect parameters in RLDP packet")
                }
                decoder
            } else if self.part > message.part as u32 {
                self.complete()?.part = message.part;
                serialize_inplace(&mut self.buf, &self.complete)?;
                return Ok(Some(&self.buf[..]));
            } else {
                return Ok(None);
            }
        } else {
            self.decoder.get_or_insert_with(|| RaptorqDecoder::with_params(*fec_type))
        };
        if let Some(mut data) = decoder.decode(message.seqno as u32, &message.data) {
            if data.len() + self.data.len() > total_size {
                fail!("Too big size for RLDP transfer")
            } else {
                self.data.append(&mut data)
            }
            if self.data.len() < total_size {
                self.decoder = None;
                self.part += 1;
                self.confirm_count = 0;
            }
            self.complete()?.part = message.part;
            serialize_inplace(&mut self.buf, &self.complete)?;
            Ok(Some(&self.buf[..]))
        } else {
            if self.confirm_count == 9 {
                let max_seqno = decoder.seqno;
                let confirm = self.confirm()?;
                confirm.part = message.part;
                confirm.seqno = max_seqno as i32;
                self.confirm_count = 0;
println!("\n\n\nConfirm {}", max_seqno);
                serialize_inplace(&mut self.buf, &self.confirm)?;
                Ok(Some(&self.buf[..]))
            } else {
                self.confirm_count += 1;
println!("\n\n\nConfirm count {} {}", self.confirm_count, decoder.seqno);
                Ok(None)
            }
        }
    }

}

/// RaptorQ encoder
pub struct RaptorqEncoder {
    encoder_index: usize,
    engine: raptorq::Encoder,
    params: FecTypeRaptorQ,
    source_packets: Vec<raptorq::EncodingPacket>
}

impl RaptorqEncoder {

    /// Construct over data
    pub fn with_data(data: &[u8]) -> Self {
        let engine = raptorq::Encoder::with_defaults(data, SendTransfer::SYMBOL as u16); 
        let mut source_packets = Vec::new();
        for encoder in engine.get_block_encoders() {
            source_packets.append(&mut encoder.source_packets())
        }
        Self {
            encoder_index: 0,
            engine,
            params: FecTypeRaptorQ {
                data_size: data.len() as i32,
                symbol_size: SendTransfer::SYMBOL as i32, 
                symbols_count: source_packets.len() as i32
            },
            source_packets
        }
    }

    /// Encode 
    pub fn encode(&mut self, seqno: &mut u32) -> Result<Vec<u8>> {
        let encoders = self.engine.get_block_encoders();
        let packet = if let Some(packet) = self.source_packets.pop() {
            packet
        } else {
            let mut packets = encoders[self.encoder_index].repair_packets(*seqno, 1);
            let packet = if let Some(packet) = packets.pop() {
                packet
            } else {
                fail!("INTERNAL ERROR: cannot encode repair packet");
            };
            self.encoder_index += 1;
            if self.encoder_index >= encoders.len() {
                self.encoder_index = 0;
            }
            packet
        };
        *seqno = packet.payload_id().encoding_symbol_id();
        Ok(packet.data().to_vec())
    }

    /// Parameters
    pub fn params(&self) -> &FecTypeRaptorQ {
        &self.params
    }

}

struct SendTransfer<'a> {
    buf: Vec<u8>,
    data: &'a [u8],
    encoder: Option<RaptorqEncoder>,
    message: RldpMessagePartBoxed,
    state: Arc<SendTransferState>
}

impl <'a> SendTransfer<'a> {

    const SLICE:  usize = 2000000;
    const SYMBOL: usize = 768;
    const WINDOW: usize = 1000;

    fn new(data: &'a [u8]) -> Self {
        let transfer_id: TransferId = rand::thread_rng().gen();
        let message = RldpMessagePart {
            transfer_id: ton::int256(transfer_id),
            fec_type: FecTypeRaptorQ {
                data_size: 0,
                symbol_size: Self::SYMBOL as i32, 
                symbols_count: 0
            }.into_boxed(),
            part: 0,
            total_size: 0,
            seqno: 0,
            data: ton::bytes(Vec::new())
        }.into_boxed();
        Self {
            buf: Vec::new(), 
            data,
            encoder: None,
            message,
            state: Arc::new(
                SendTransferState {
                    part: AtomicU32::new(0),
                    reply: AtomicBool::new(false),
                    seqno_sent: AtomicU32::new(0),
                    seqno_recv: AtomicU32::new(0)
                }
            )
        }
    }

    fn is_finished(&self) -> bool {
        self.state.has_reply() && 
        ((self.state.part() as usize + 1) * Self::SLICE >= self.data.len())
    }

    fn message(&mut self) -> Result<&mut RldpMessagePart> {
        match self.message { 
            RldpMessagePartBoxed::Rldp_MessagePart(ref mut msg) => Ok(msg),
            _ => fail!("INTERNAL ERROR: RLDP message mismatch")
        }
    }

    fn prepare_chunk(&mut self) -> Result<&[u8]> {     
        if let Some(encoder) = &mut self.encoder {
            let mut seqno_sent = self.state.seqno_sent();
            let chunk = encoder.encode(&mut seqno_sent)?;
            let message = self.message()?;
            message.seqno = seqno_sent as i32;
            message.data = ton::bytes(chunk);
            let seqno_recv = self.state.seqno_recv();
            if seqno_sent - seqno_recv <= Self::WINDOW as u32 {
                seqno_sent += 1;
            }
            self.state.set_seqno_sent(seqno_sent);
            serialize_inplace(&mut self.buf, &self.message)?;
            Ok(&self.buf[..])        
        } else {
            fail!("Encoder is not ready");
        }
    }

    fn start_next_part(&mut self) -> Result<bool> {
        if self.is_finished() {
           return Ok(false);
        }
        let part = self.state.part() as usize;
        let processed = part * Self::SLICE;
        let total = self.data.len();
        if processed >= total {
           return Ok(false);
        }
        let chunk_size = min(total - processed, Self::SLICE);
        let encoder = RaptorqEncoder::with_data(
            &self.data[processed..processed + chunk_size]
        );
        let message = self.message()?;
        message.part = part as i32;
        message.total_size = total as i64;
        match message.fec_type {
            FecType::Fec_RaptorQ(ref mut fec_type) => {
                fec_type.data_size = encoder.params.data_size;      
                fec_type.symbols_count = encoder.params.symbols_count;
            },
            _ => fail!("INTERNAL ERROR: unsupported FEC type")
        }
        self.encoder = Some(encoder);
        Ok(true)
    }

}

struct SendTransferState {
    part: AtomicU32,
    reply: AtomicBool,
    seqno_sent: AtomicU32,
    seqno_recv: AtomicU32
}

impl SendTransferState {
    fn has_reply(&self) -> bool {
        self.reply.load(Ordering::Relaxed)
    }
    fn part(&self) -> u32 {
        self.part.load(Ordering::Relaxed)
    }
    fn seqno_recv(&self) -> u32 {
        self.seqno_recv.load(Ordering::Relaxed)
    }
    fn seqno_sent(&self) -> u32 {
        self.seqno_sent.load(Ordering::Relaxed)
    }
    fn set_part(&self, part: u32) {
        self.part.compare_and_swap(part - 1, part, Ordering::Relaxed);
    }
    fn set_reply(&self) {
        self.reply.store(true, Ordering::Relaxed)
    }
    fn set_seqno_recv(&self, seqno: u32) {
        if self.seqno_sent() >= seqno {
            let seqno_recv = self.seqno_recv();
            if seqno_recv < seqno {
                self.seqno_recv.compare_and_swap(seqno_recv, seqno, Ordering::Relaxed);
            }
        }
    }
    fn set_seqno_sent(&self, seqno: u32) {
        let seqno_sent = self.seqno_sent();
        if seqno_sent < seqno {
            self.seqno_sent.compare_and_swap(seqno_sent, seqno, Ordering::Relaxed);
        }
    }
}

enum RldpTransfer {
    Recv(tokio::sync::mpsc::UnboundedSender<Box<RldpMessagePart>>),
    Send(Arc<SendTransferState>)
}

type Transfers = lockfree::map::Map<TransferId, RldpTransfer>;

/// Rldp Node
pub struct RldpNode {
    adnl: Arc<AdnlNode>,
    transfers: Transfers
}

impl RldpNode {

    const TIMEOUT: u64 = 10000;    // Milliseconds
    const SPINNER: u32 = 10000000; // Nanoseconds

    /// Constructor 
    pub fn with_adnl_node(adnl: Arc<AdnlNode>) -> Result<Arc<Self>> {
        let ret = Self {
            adnl,
            transfers: lockfree::map::Map::new()
        };
        Ok(Arc::new(ret))
    }

    /// Send query 
    pub async fn query(
        &self, 
        dst: &Arc<KeyId>, 
        src: &Arc<KeyId>, 
        data: &[u8],
        max_answer_size: Option<i64>
    ) -> Result<Option<Vec<u8>>> {
        let query_id: QueryId = rand::thread_rng().gen();
        let message = RldpQuery {
            query_id: ton::int256(query_id.clone()),
            max_answer_size: max_answer_size.unwrap_or(128 * 1024),
            timeout: Version::get() + Self::TIMEOUT as i32/1000,
            data: ton::bytes(data.to_vec())
        }.into_boxed();
        if let Some(answer) = self.transfer(dst, src, &serialize(&message)?).await? {
            match deserialize(&answer[..])?.downcast::<RldpMessageBoxed>() {
                Ok(RldpMessageBoxed::Rldp_Answer(answer)) => if answer.query_id.0 != query_id {
                    fail!("Unknown query ID in RLDP answer")
                } else {
                    Ok(Some(answer.data.to_vec()))
                },
                Ok(answer) => 
                    fail!("Unexpected answer to RLDP query: {:?}", answer),
                Err(answer) => 
                    fail!("Unexpected answer to RLDP query: {:?}", answer)
            }
        } else {
            Ok(None)
        }        
    }

    async fn transfer(
        &self, 
        dst: &Arc<KeyId>, 
        src: &Arc<KeyId>,
        data: &[u8]
    ) -> Result<Option<Vec<u8>>> {  
        let send_transfer = SendTransfer::new(data);
        let send_transfer_id = send_transfer.message.transfer_id().0.clone();
        self.transfers.insert(
            send_transfer_id.clone(), 
            RldpTransfer::Send(send_transfer.state.clone())
        );
        let mut recv_transfer_id = send_transfer_id.clone();
        for i in 0..recv_transfer_id.len() {
            recv_transfer_id[i] ^= 0xFF
        } 
        let (queue_sender, queue_reader) = tokio::sync::mpsc::unbounded_channel();
        let recv_transfer = RecvTransfer::new(recv_transfer_id.clone());
        self.transfers.insert(
            recv_transfer_id.clone(), 
            RldpTransfer::Recv(queue_sender)
        );
        let ret = self
            .transfer_loop(send_transfer, recv_transfer, queue_reader, dst, src)
            .await;
        self.transfers.remove(&send_transfer_id); 
        self.transfers.remove(&recv_transfer_id); 
        ret
    }

    async fn transfer_loop(
        &self, 
        mut send_transfer: SendTransfer<'_>, 
        mut recv_transfer: RecvTransfer, 
        mut queue_reader: tokio::sync::mpsc::UnboundedReceiver<Box<RldpMessagePart>>,
        dst: &Arc<KeyId>,
        src: &Arc<KeyId>
    ) -> Result<Option<Vec<u8>>> {
log::debug!(target: TARGET, "total {}", send_transfer.data.len());
log::debug!(target: TARGET, "transfer {}", base64::encode(&send_transfer.message.transfer_id().0));
        let adnl = self.adnl.clone();
        let ping = Arc::new(lockfree::queue::Queue::new());
        let pong = ping.clone();
        let dest = dst.clone();
        let sorc = src.clone();
        let send = send_transfer.state.clone();
        tokio::spawn(
            async move {
                while let Some(job) = queue_reader.recv().await {
//println!("JOB-R {:?}", job);
                    let begin = recv_transfer.data.len() == 0;
                    match recv_transfer.process_chunk(job) {
                        Err(e) => log::warn!("RLDP error: {}", e),
                        Ok(Some(reply)) => match adnl.send_custom(&dest, &sorc, reply).await {
                            Err(e) => log::warn!("RLDP error: {}", e),
                            _ => ()
                        },
                        _ => ()
                    }
                    send.set_reply();                    
                    if begin && (recv_transfer.data.len() > 0) {
println!("FIRST decoded {}", recv_transfer.data.len());
                        dump!(trace, TARGET, "PACKET #0", &recv_transfer.data[..32]);
                    }
                    if let Some(total_size) = recv_transfer.total_size {
                        if total_size == recv_transfer.data.len() {
                            pong.push(recv_transfer);
log::warn!(target: TARGET, "QUEUEEED!");
                            break
                        }
                    } else {
                        log::warn!("INTERNAL ERROR: RLDP total size mismatch")
                    }
                }   
                // Graceful close
                queue_reader.close();
                while let Some(_) = queue_reader.recv().await {
                }
log::warn!(target: TARGET, "CLOOOOSED!");
            }
        );
        let start = Instant::now();
        let mut count = 0;
        loop {
            if !send_transfer.start_next_part()? {
                break;
            }
            let part = send_transfer.state.part();
            loop {                            
                for _ in 0..10 {
                    self.adnl.send_custom(dst, src, send_transfer.prepare_chunk()?).await?;
                }                                                                                                                         
                tokio::time::delay_for(Duration::new(0, Self::SPINNER)).await;
                if start.elapsed().as_secs() as u64 >= count {
                    count += 1;
log::warn!(target: TARGET, "Seqno sent/recv {}/{} ", 
    send_transfer.state.seqno_sent.load(Ordering::Relaxed),
    send_transfer.state.seqno_recv.load(Ordering::Relaxed)
);
                }
                if send_transfer.is_finished() {
                    break;
                }
                if start.elapsed().as_millis() as u64 > Self::TIMEOUT {
                    return Ok(None)
                }
                match send_transfer.state.part() {
                    x if x == part => continue,
                    x if x == part + 1 => break,
                    _ => fail!("INTERNAL ERROR: part # mismatch")
                }
            }
        }
        self.transfers.remove(&send_transfer.message.transfer_id().0);
log::warn!(target: TARGET, "READY!");
        loop {
            tokio::time::delay_for(Duration::new(0, Self::SPINNER)).await;
            if start.elapsed().as_millis() as u64 > Self::TIMEOUT {
                break
            }
            if let Some(reply) = ping.pop() {
log::warn!(target: TARGET, "REPLIIIED!");
//                self.transfers.remove(&recv_transfer_id);
                return Ok(Some(reply.data))
            }
        }
//        self.transfers.remove(&recv_transfer_id);
        Ok(None)
    }

}

impl Subscriber for RldpNode {
    fn try_consume_custom(&self, data: &[u8]) -> Result<bool> {
        let msg = if let Ok(msg) = deserialize(data) {
            msg
        } else {
            return Ok(false)
        };
        let msg = if let Ok(msg) = msg.downcast::<RldpMessagePartBoxed>() { 
            msg
        } else {
            return Ok(false)
        };
        match msg {
            RldpMessagePartBoxed::Rldp_Complete(msg) => {
                if let Some(transfer) = self.transfers.get(&msg.transfer_id.0) {
                    if let RldpTransfer::Send(transfer) = transfer.val() {
                        transfer.set_part(msg.part as u32 + 1);
                    }
                }
            },
            RldpMessagePartBoxed::Rldp_Confirm(msg) => {
                if let Some(transfer) = self.transfers.get(&msg.transfer_id.0) {
                    if let RldpTransfer::Send(transfer) = transfer.val() {                        
                        if transfer.part() == msg.part as u32 {
                            transfer.set_seqno_recv(msg.seqno as u32);
                        }
                    }
                }
            },
            RldpMessagePartBoxed::Rldp_MessagePart(msg) => {
//println!("MSG-R {:?}", msg);                     
                if let Some(transfer) = self.transfers.get(&msg.transfer_id.0) {
                    if let RldpTransfer::Recv(queue_sender) = transfer.val() {   
//println!("MSG-S {:?}", msg);                     
                        match queue_sender.send(msg) {
                            Ok(()) => (),
                            Err(tokio::sync::mpsc::error::SendError(_)) => ()
                        }
                    }
                }
            }
        }
        Ok(true) 
    }
}
