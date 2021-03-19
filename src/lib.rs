use adnl::{ 
    dump, 
    common::{
        add_object_to_map, deserialize, get256, AdnlPeers, KeyId, Query, QueryId, serialize, 
        serialize_inplace, Subscriber, Version
    },
    node::AdnlNode
};
use rand::Rng;
use std::{
    cmp::{min, max}, sync::{Arc, atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering}}, 
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
    state: Arc<RecvTransferState>,
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
            state: Arc::new(
                RecvTransferState {
                    updates: AtomicU32::new(0)
                }
            ),
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
                serialize_inplace(&mut self.buf, &self.confirm)?;
                Ok(Some(&self.buf[..]))
            } else {
                self.confirm_count += 1;
                Ok(None)
            }
        }
    }

}

struct RecvTransferState {
    updates: AtomicU32
}

impl RecvTransferState {
    fn updates(&self) -> u32 {
        self.updates.load(Ordering::Relaxed)
    }
    fn set_updates(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
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
            // Reverse order to send efficiently
            let mut packets = encoder.source_packets();
            while let Some(packet) = packets.pop() {
                source_packets.push(packet)
            }
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

    fn new(data: &'a [u8], transfer_id: Option<TransferId>) -> Self {
        let transfer_id = transfer_id.unwrap_or_else(
            || rand::thread_rng().gen()
        );
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
            let seqno_sent_original = seqno_sent;
            let chunk = encoder.encode(&mut seqno_sent)?;
            let message = self.message()?;
            message.seqno = seqno_sent as i32;
            message.data = ton::bytes(chunk);
            let seqno_recv = self.state.seqno_recv();
            if seqno_sent - seqno_recv <= Self::WINDOW as u32 {
                if seqno_sent_original == seqno_sent {
                    seqno_sent += 1;
                }
                self.state.set_seqno_sent(seqno_sent);
            }
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
    Send(Arc<SendTransferState>),
    Done
}

struct RldpRecvContext {
    adnl: Arc<AdnlNode>, 
    peers: AdnlPeers,
    queue_reader: tokio::sync::mpsc::UnboundedReceiver<Box<RldpMessagePart>>,
    recv_transfer: RecvTransfer,
    transfer_id: TransferId
}

struct RldpSendContext<'a> {
    adnl: Arc<AdnlNode>, 
    peers: AdnlPeers,
    send_transfer: SendTransfer<'a>,
    transfer_id: TransferId
}

#[derive(Default)]
struct RldpStats {
    transfers_sent_all: AtomicU64,
    transfers_recv_all: AtomicU64,
    transfers_sent_now: AtomicU64,
    transfers_recv_now: AtomicU64
}

impl RldpStats {
    fn inc(stat: &AtomicU64) -> u64 {
        stat.fetch_add(1, Ordering::Relaxed) + 1
    }
    fn dec(stat: &AtomicU64) -> u64 {
        stat.fetch_sub(1, Ordering::Relaxed) - 1
    }
}

struct RldpPeer {
    queries: AtomicU32,
    queue: lockfree::queue::Queue<Arc<tokio::sync::Barrier>>
}

/// Rldp Node
pub struct RldpNode {
    adnl: Arc<AdnlNode>,
    peers: lockfree::map::Map<Arc<KeyId>, RldpPeer>,
    stats: Arc<RldpStats>,
    subscribers: Arc<Vec<Arc<dyn Subscriber>>>,
    transfers: Arc<lockfree::map::Map<TransferId, RldpTransfer>>
}

impl RldpNode {

    const SPINNER: u64 = 10;       // Milliseconds
    const TIMEOUT: u64 = 10000;    // Milliseconds

    /// Constructor 
    pub fn with_adnl_node(
        adnl: Arc<AdnlNode>, 
        subscribers: Vec<Arc<dyn Subscriber>>
    ) -> Result<Arc<Self>> {
        let ret = Self {
            adnl,
            peers: lockfree::map::Map::new(), 
            stats: Arc::new(RldpStats::default()),
            subscribers: Arc::new(subscribers),
            transfers: Arc::new(lockfree::map::Map::new())
        };
        Ok(Arc::new(ret))
    }

    /// Send query 
    pub async fn query(
        &self, 
        data: &[u8],
        max_answer_size: Option<i64>,
        peers: &AdnlPeers,
        roundtrip: Option<u64>
    ) -> Result<(Option<Vec<u8>>, u64)> {
        let query_id: QueryId = rand::thread_rng().gen();
        let message = RldpQuery {
            query_id: ton::int256(query_id.clone()),
            max_answer_size: max_answer_size.unwrap_or(128 * 1024),
            timeout: Version::get() + Self::TIMEOUT as i32/1000,
            data: ton::bytes(data.to_vec())
        }.into_boxed();
        let msg = serialize(&message)?;
        let peer = if let Some(peer) = self.peers.get(peers.other()) {
            peer
        } else {
            add_object_to_map(
                &self.peers, 
                peers.other().clone(),
                || {
                    let ret = RldpPeer {
                        queries: AtomicU32::new(0),
                        queue: lockfree::queue::Queue::new()
                    };
                    Ok(ret)
                }
            )?;
            if let Some(peer) = self.peers.get(peers.other()) {
                peer
            } else {
                fail!("Cannot find RLDP peer {}", peers.other())
            }           
        };
        let peer = peer.val();
        if peer.queries.fetch_add(1, Ordering::Relaxed) != 0 {
            let ping = Arc::new(tokio::sync::Barrier::new(2));
            peer.queue.push(ping.clone());
            ping.wait().await;
        }                                                 	
        let all = RldpStats::inc(&self.stats.transfers_sent_all);
        let now = RldpStats::inc(&self.stats.transfers_sent_now);
        log::trace!(target: TARGET, "RLDP send stats: all {}, now {}", all, now);
        let res = self.query_transfer(&msg, peers, Self::calc_timeout(roundtrip)).await;
        let all = self.stats.transfers_sent_all.load(Ordering::Relaxed);
        let now = RldpStats::dec(&self.stats.transfers_sent_now);
        log::trace!(target: TARGET, "RLDP send stats: all {}, now {}", all, now);
        if peer.queries.fetch_sub(1, Ordering::Relaxed) > 1 {
            loop {
                if let Some(pong) = peer.queue.pop() {
                    pong.wait().await;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(Self::SPINNER)).await;
            }
        }
        let (answer, roundtrip) = res?;
        if let Some(answer) = answer {
            match deserialize(&answer[..])?.downcast::<RldpMessageBoxed>() {
                Ok(RldpMessageBoxed::Rldp_Answer(answer)) => if answer.query_id.0 != query_id {
                    fail!("Unknown query ID in RLDP answer")
                } else {
                    log::trace!(
                        target: TARGET, 
                        "RLDP answer {:02x}{:02x}{:02x}{:02x}...", 
                        answer.data[0], answer.data[1], answer.data[2], answer.data[3]
                    );
                    Ok((Some(answer.data.to_vec()), roundtrip))
                },
                Ok(answer) => 
                    fail!("Unexpected answer to RLDP query: {:?}", answer),
                Err(answer) => 
                    fail!("Unexpected answer to RLDP query: {:?}", answer)
            }
        } else {
            Ok((None, roundtrip))
        }        
    }

    fn answer_transfer(
        &self, 
        transfer_id: &TransferId, 
        peers: &AdnlPeers
    ) -> Result<Option<tokio::sync::mpsc::UnboundedSender<Box<RldpMessagePart>>>> {
        let (queue_sender, queue_reader) = tokio::sync::mpsc::unbounded_channel();
        let inserted = add_object_to_map(
            &self.transfers, 
            transfer_id.clone(),
            || Ok(RldpTransfer::Recv(queue_sender.clone()))
        )?;
        if !inserted {
            return Ok(None)
        }
        let all = RldpStats::inc(&self.stats.transfers_recv_all);
        let now = RldpStats::inc(&self.stats.transfers_recv_now);
        log::trace!(target: TARGET, "RLDP recv stats: all {}, now {}", all, now);
        let mut context = RldpRecvContext {
            adnl: self.adnl.clone(),
            peers: peers.clone(),
            queue_reader,
            recv_transfer: RecvTransfer::new(transfer_id.clone()),
            transfer_id: transfer_id.clone()
        };
        let stats = self.stats.clone();
        let subscribers = self.subscribers.clone();
        let transfers = self.transfers.clone();
        tokio::spawn(
            async move {
                Self::receive_loop(&mut context, None).await;
                transfers.insert(context.transfer_id.clone(), RldpTransfer::Done);
                let send_transfer_id = Self::answer_transfer_loop(
                    &mut context, 
                    subscribers, 
                    transfers.clone()
                ).await.unwrap_or_else(
                    |e| {
                        log::warn!(
                            target: TARGET, 
                            "ERROR: {}, transfer {}", 
                            e, base64::encode(&context.transfer_id)
                        );
                        None
                    },
                );    
                let all = stats.transfers_recv_all.load(Ordering::Relaxed);
                let now = RldpStats::dec(&stats.transfers_recv_now);
                log::trace!(target: TARGET, "RLDP recv stats: all {}, now {}", all, now);
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT * 2)).await;
                if let Some(send_transfer_id) = send_transfer_id {
                    transfers.remove(&send_transfer_id);
                }
                transfers.remove(&context.transfer_id);
            }
        );
        let transfers = self.transfers.clone();
        let transfer_id = transfer_id.clone();
        tokio::spawn(
            async move {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT)).await;
                transfers.insert(transfer_id, RldpTransfer::Done);
            }
        );
        Ok(Some(queue_sender))
    }

    async fn answer_transfer_loop(
        context: &mut RldpRecvContext, 
        subscribers: Arc<Vec<Arc<dyn Subscriber>>>,
        transfers: Arc<lockfree::map::Map<TransferId, RldpTransfer>>,
    ) -> Result<Option<TransferId>> { 
        let query = match deserialize(
            &context.recv_transfer.data[..]
        )?.downcast::<RldpMessageBoxed>() {
            Ok(RldpMessageBoxed::Rldp_Query(query)) => query,
            Ok(message) => fail!("Unexpected RLDP message: {:?}", message),
            Err(object) => fail!("Unexpected RLDP message: {:?}", object)
        };
        let answer = if let (true, answer) = Query::process_rldp(
            &subscribers, 
            &query, 
            &context.peers
        ).await? {
            if let Some(answer) = answer {
                answer
            } else {
                return Ok(None)
            }
        } else {
            fail!("No subscribers for query {:?}", query)
        };
        let (len, max) = (answer.data.len(), query.max_answer_size as usize);
        if len > max {
            fail!("Exceeded max RLDP answer size: {} vs {}", len, max)
        }
        let data = serialize(&answer.into_boxed())?;
        let mut send_transfer_id = context.transfer_id.clone();
        for i in 0..send_transfer_id.len() {
            send_transfer_id[i] ^= 0xFF
        } 
        log::trace!(
            target: TARGET, 
            "RLDP answer to be sent in transfer {}/{} to {}",
            base64::encode(&context.transfer_id),
            base64::encode(&send_transfer_id),
            context.peers.other()  
        );
        let send_transfer = SendTransfer::new(&data[..], Some(send_transfer_id.clone()));
        transfers.insert(
            send_transfer_id.clone(), 
            RldpTransfer::Send(send_transfer.state.clone())
        );
        let context_send = RldpSendContext {
            adnl: context.adnl.clone(),
            peers: context.peers.clone(),
            send_transfer,
            transfer_id: context.transfer_id.clone()
        };
        if let (true, _) = Self::send_loop(context_send, Self::calc_timeout(None)).await? {
            log::trace!(
                target: TARGET, 
                "RLDP answer sent in transfer {} to {}",
                base64::encode(&context.transfer_id),
                context.peers.other()  
            )
        } else {
            log::warn!(
                target: TARGET, 
                "Timeout on answer in RLDP transfer {} to {}", 
                base64::encode(&context.transfer_id),
                context.peers.other()  
            ) 
        }
        Ok(Some(send_transfer_id))
    }

    fn calc_timeout(roundtrip: Option<u64>) -> u64 {
        max(roundtrip.unwrap_or(Self::TIMEOUT), Self::SPINNER * 2)
    }

    fn is_timed_out(timeout: u64, updates: u32, start: &Instant) -> bool {
        start.elapsed().as_millis() as u64 > timeout + timeout * updates as u64 / 100
    }

    async fn query_transfer(
        &self, 
        data: &[u8],
        peers: &AdnlPeers,
        timeout: u64
    ) -> Result<(Option<Vec<u8>>, u64)> {  
        let send_transfer = SendTransfer::new(data, None);
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
        let send_context = RldpSendContext {
            adnl: self.adnl.clone(),
            peers: peers.clone(),
            send_transfer,
            transfer_id: send_transfer_id.clone()
        }; 
        let recv_context = RldpRecvContext {
            adnl: self.adnl.clone(),
            peers: peers.clone(),
            queue_reader,
            recv_transfer,
            transfer_id: send_transfer_id.clone()
        }; 
        log::trace!(
            target: TARGET, 
            "transfer id {}/{}, total to send {}", 
            base64::encode(&send_transfer_id), 
            base64::encode(&recv_transfer_id), 
            data.len()
        );
        let ret = self
            .query_transfer_loop(send_context, recv_context, timeout)
            .await;
        if ret.is_err() {
            self.transfers.insert(send_transfer_id, RldpTransfer::Done);
        }
        self.transfers.insert(recv_transfer_id, RldpTransfer::Done);
        let transfers = self.transfers.clone();
        tokio::spawn(
            async move {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT * 2)).await;
                transfers.remove(&send_transfer_id); 
                transfers.remove(&recv_transfer_id); 
            }
        );        
        ret
    }

    async fn query_transfer_loop(
        &self, 
        send_context: RldpSendContext<'_>, 
        mut recv_context: RldpRecvContext, 
        timeout: u64,
    ) -> Result<(Option<Vec<u8>>, u64)> {
        let ping = Arc::new(lockfree::queue::Queue::new());
        let pong = ping.clone();
        let peers = send_context.peers.clone();
        let recv_state = recv_context.recv_transfer.state.clone();
        let send_state = send_context.send_transfer.state.clone();
        let transfer_id = send_context.transfer_id.clone();
        tokio::spawn(
            async move {
                Self::receive_loop(&mut recv_context, Some(send_state)).await;
                pong.push(recv_context.recv_transfer)
            }
        );
        let (ok, mut timeout) = Self::send_loop(send_context, timeout).await?;
        self.transfers.insert(transfer_id.clone(), RldpTransfer::Done);
        if ok {
            log::trace!(
                target: TARGET, 
                "RLDP query sent in transfer {} to {}, waiting for answer",
                base64::encode(&transfer_id),
                peers.other()
            )
        } else {
            log::warn!(
                target: TARGET, 
                "Timeout ({} ms) on query in RLDP transfer {} to {}",
                timeout,
                base64::encode(&transfer_id),
                peers.other()
            );
            return Ok((None, timeout));
        }
        let mut start_part = Instant::now();
        let mut updates = recv_state.updates();
        loop {
            tokio::time::sleep(Duration::from_millis(Self::SPINNER)).await;
            let new_updates = recv_state.updates();
            if new_updates > updates {
                log::trace!(
                    target: TARGET, 
                    "Recv updates {} -> {} in transfer {}", 
                    updates, new_updates, base64::encode(&transfer_id)
                );
                timeout = Self::update_timeout(timeout, &start_part);
                updates = new_updates;
                start_part = Instant::now();
            } else if Self::is_timed_out(timeout, updates, &start_part) {
                log::warn!(
                    target: TARGET, 
                    "No activity for transfer {} to {} in {} ms, aborting", 
                    base64::encode(&transfer_id),
                    peers.other(),
                    timeout
                );
                break
            }
            if let Some(reply) = ping.pop() {
                log::trace!(                  
                    target: TARGET, 
                    "Got reply for transfer {} from {}", 
                    base64::encode(&transfer_id),
                    peers.other()
                );
                timeout = Self::update_timeout(timeout, &start_part);
                return Ok((Some(reply.data), timeout))
            }
        }
        Ok((None, timeout))
    }

    async fn receive_loop(
        context: &mut RldpRecvContext,
        mut send_state: Option<Arc<SendTransferState>>
    ) {
        while let Some(job) = context.queue_reader.recv().await {
            let begin = context.recv_transfer.data.len() == 0;
            match context.recv_transfer.process_chunk(job) {
                Err(e) => log::warn!("RLDP error: {}", e),
                Ok(Some(reply)) => match context.adnl.send_custom(reply, &context.peers).await {
                    Err(e) => log::warn!("RLDP error: {}", e),
                    _ => ()
                },
                _ => ()
            }
            context.recv_transfer.state.set_updates();
            if let Some(send_state) = send_state.take() {
                send_state.set_reply();                    
            }
            if begin && (context.recv_transfer.data.len() > 0) {
                log::trace!(
                    target: TARGET,
                    "transfer id {}, received first {}, total to receive {:?}",
                    base64::encode(&context.transfer_id),
                    context.recv_transfer.data.len(),
                    context.recv_transfer.total_size
                );              
                let len = min(64, context.recv_transfer.data.len());              
                dump!(trace, TARGET, "PACKET #0", &context.recv_transfer.data[..len]);
            }
            if let Some(total_size) = context.recv_transfer.total_size {
                if total_size == context.recv_transfer.data.len() {
                    log::trace!(
                        target: TARGET, 
                        "transfer id {}, receive completed ({})",
                        base64::encode(&context.transfer_id),
                        total_size,
                    );
                    break
                }
            } else {
                log::warn!("INTERNAL ERROR: RLDP total size mismatch")
            }
        }   
        // Graceful close
        context.queue_reader.close();
        while let Some(_) = context.queue_reader.recv().await {
        }
    }

    async fn send_loop(mut context: RldpSendContext<'_>, mut timeout: u64) -> Result<(bool, u64)> {
        loop {
            if !context.send_transfer.start_next_part()? {
                break;
            }
            let part = context.send_transfer.state.part();
            let mut start_part = Instant::now();
            let mut recv_seqno = 0;
            loop {                            
                for _ in 0..10 {
                    context.adnl.send_custom(
                        context.send_transfer.prepare_chunk()?, 
                        &context.peers
                    ).await?;
                }                                                                                                                         
                tokio::time::sleep(Duration::from_millis(Self::SPINNER)).await;
                if context.send_transfer.is_finished() {
                    break;
                }
                let new_recv_seqno = context.send_transfer.state.seqno_recv();
                if new_recv_seqno > recv_seqno {
                    log::trace!(
                        target: TARGET, 
                        "Send updates {} -> {} in transfer {}", 
                        recv_seqno, new_recv_seqno, base64::encode(&context.transfer_id)
                    );
                    timeout = Self::update_timeout(timeout, &start_part);
                    recv_seqno = new_recv_seqno;
                    start_part = Instant::now();
                } else if Self::is_timed_out(timeout, recv_seqno, &start_part) {
                    return Ok((false, timeout))
                }                
                match context.send_transfer.state.part() {
                    x if x == part => continue,
                    x if x == part + 1 => break,
                    _ => fail!("INTERNAL ERROR: part # mismatch")
                }
            }
            timeout = Self::update_timeout(timeout, &start_part);
        }
        Ok((true, timeout))
    }

    fn update_timeout(timeout: u64, start: &Instant) -> u64{
        (timeout + start.elapsed().as_millis() as u64) / 2
    }

}

#[async_trait::async_trait]
impl Subscriber for RldpNode {
    async fn try_consume_custom(&self, data: &[u8], peers: &AdnlPeers) -> Result<bool> {
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
                let transfer_id = get256(&msg.transfer_id);
                loop {
                    let result = if let Some(transfer) = self.transfers.get(transfer_id) {
                        if let RldpTransfer::Recv(queue_sender) = transfer.val() {   
                            queue_sender.send(msg)
                        } else {
                            break
                        }
                    } else {
                        if let Some(queue_sender) = self.answer_transfer(transfer_id, peers)? {
                            queue_sender.send(msg)
                        } else {
                            continue
                        }
                    };
                    match result {
                        Ok(()) => (),
                        Err(tokio::sync::mpsc::error::SendError(_)) => ()
                    }
                    break
                }
            }
        }
        Ok(true) 
    }
}
