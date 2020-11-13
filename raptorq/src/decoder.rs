use crate::base::intermediate_tuple;
use crate::base::partition;
use crate::base::EncodingPacket;
use crate::base::ObjectTransmissionInformation;
use crate::base::PayloadId;
use crate::constraint_matrix::enc_indices;
use crate::constraint_matrix::generate_constraint_matrix;
use crate::encoder::SPARSE_MATRIX_THRESHOLD;
use crate::matrix::{BinaryMatrix, DenseBinaryMatrix};
use crate::octet_matrix::DenseOctetMatrix;
use crate::pi_solver::fused_inverse_mul_symbols;
use crate::sparse_matrix::SparseBinaryMatrix;
use crate::symbol::Symbol;
use crate::systematic_constants::num_hdpc_symbols;
use crate::systematic_constants::num_ldpc_symbols;
use crate::systematic_constants::{
    calculate_p1, extended_source_block_symbols, num_lt_symbols, num_pi_symbols, systematic_index,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Decoder {
    config: ObjectTransmissionInformation,
    block_decoders: Vec<SourceBlockDecoder>,
    blocks: Vec<Option<Vec<u8>>>,
}

impl Decoder {
    pub fn new(config: ObjectTransmissionInformation) -> Decoder {
        let kt = (config.transfer_length() as f64 / config.symbol_size() as f64).ceil() as u32;
        let (kl, ks, zl, zs) = partition(kt, config.source_blocks());

        // TODO: support subblocks
        assert_eq!(1, config.sub_blocks());
        //        let (tl, ts, nl, ns) = partition((config.symbol_size() / config.alignment() as u16) as u32, config.sub_blocks());

        let mut decoders = vec![];
        for i in 0..zl {
            decoders.push(SourceBlockDecoder::new(
                i as u8,
                config.symbol_size(),
                u64::from(kl) * u64::from(config.symbol_size()),
            ));
        }

        for i in 0..zs {
            decoders.push(SourceBlockDecoder::new(
                i as u8,
                config.symbol_size(),
                u64::from(ks) * u64::from(config.symbol_size()),
            ));
        }

        Decoder {
            config,
            block_decoders: decoders,
            blocks: vec![None; (zl + zs) as usize],
        }
    }

    #[cfg(any(test, feature = "benchmarking"))]
    pub fn set_sparse_threshold(&mut self, value: u32) {
        for block_decoder in self.block_decoders.iter_mut() {
            block_decoder.set_sparse_threshold(value);
        }
    }

    pub fn decode(&mut self, packet: EncodingPacket) -> Option<Vec<u8>> {
        let block_number = packet.payload_id.source_block_number() as usize;
        if self.blocks[block_number].is_none() {
            self.blocks[block_number] = self.block_decoders[block_number].decode(vec![packet]);
        }
        for block in self.blocks.iter() {
            if block.is_none() {
                return None;
            }
        }

        let mut result = vec![];
        for block in self.blocks.iter() {
            result.extend(block.clone().unwrap());
        }
        result.truncate(self.config.transfer_length() as usize);
        Some(result)
    }

    pub fn add_new_packet(&mut self, packet: EncodingPacket) {
        let block_number = packet.payload_id.source_block_number() as usize;
        if self.blocks[block_number].is_none() {
            self.blocks[block_number] = self.block_decoders[block_number].decode(vec![packet]);
        }
    }

    pub fn get_result(&self) -> Option<Vec<u8>> {
        for block in self.blocks.iter() {
            if block.is_none() {
                return None;
            }
        }

        let mut result = vec![];
        for block in self.blocks.iter() {
            result.extend(block.clone().unwrap());
        }
        result.truncate(self.config.transfer_length() as usize);
        Some(result)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceBlockDecoder {
    source_block_id: u8,
    symbol_size: u16,
    source_block_symbols: u32,
    source_symbols: Vec<Option<Symbol>>,
    repair_packets: Vec<EncodingPacket>,
    received_source_symbols: u32,
    received_esi: HashSet<u32>,
    decoded: bool,
    custom: bool,
    sparse_threshold: u32,
}

impl SourceBlockDecoder {
    pub fn new(source_block_id: u8, symbol_size: u16, block_length: u64) -> SourceBlockDecoder {
        let source_symbols = (block_length as f64 / symbol_size as f64).ceil() as u32;
        let mut received_esi = HashSet::new();
        for i in source_symbols..extended_source_block_symbols(source_symbols) {
            received_esi.insert(i);
        }
        SourceBlockDecoder {
            source_block_id,
            symbol_size,
            source_block_symbols: source_symbols,
            source_symbols: vec![None; source_symbols as usize],
            repair_packets: vec![],
            received_source_symbols: 0,
            received_esi,
            decoded: false,       
            custom: false,
            sparse_threshold: SPARSE_MATRIX_THRESHOLD,
        }
    }

    #[cfg(any(test, feature = "benchmarking"))]
    pub fn set_sparse_threshold(&mut self, value: u32) {
        self.sparse_threshold = value;
    }

    fn try_pi_decode(
        &mut self,
        constraint_matrix: impl BinaryMatrix,
        hdpc_rows: DenseOctetMatrix,
        symbols: Vec<Symbol>,
    ) -> Option<Vec<u8>> {
        let intermediate_symbols = match fused_inverse_mul_symbols(
            constraint_matrix,
            hdpc_rows,
            symbols,
            self.source_block_symbols,
        ) {
            (None, _) => return None,
            (Some(s), _) => s,
        };

        let mut result = vec![];
        let lt_symbols = num_lt_symbols(self.source_block_symbols);
        let pi_symbols = num_pi_symbols(self.source_block_symbols);
        let sys_index = systematic_index(self.source_block_symbols);
        let p1 = calculate_p1(self.source_block_symbols, pi_symbols);
        for i in 0..self.source_block_symbols as usize {
            if let Some(ref symbol) = self.source_symbols[i] {
                result.extend(symbol.as_bytes())
            } else {
                let rebuilt = self.rebuild_source_symbol(
                    &intermediate_symbols,
                    i as u32,
                    lt_symbols,
                    pi_symbols,
                    sys_index,
                    p1,
                );
                result.extend(rebuilt.as_bytes());
            }
        }

        self.decoded = true;
        return Some(result);
    }

    pub fn decode<T: IntoIterator<Item = EncodingPacket>>(
        &mut self,
        packets: T,
    ) -> Option<Vec<u8>> {

        let num_extended_symbols = extended_source_block_symbols(self.source_block_symbols);

        for packet in packets {
            assert_eq!(
                self.source_block_id,
                packet.payload_id.source_block_number()
            );

            let (payload_id, payload) = packet.split();
            let symbol_id = if payload_id.encoding_symbol_id() >= self.source_block_symbols { 
                payload_id.encoding_symbol_id() + num_extended_symbols - self.source_block_symbols 
            } else {
                payload_id.encoding_symbol_id()
            };

//println!("EXTSYMBOLS {}/{}", num_extended_symbols, self.source_block_symbols);
            if self.received_esi.insert(symbol_id) {
                if symbol_id >= num_extended_symbols {
//println!("REPAIR {}", symbol_id);
                    // Repair symbol
                    self.repair_packets.push(
                        EncodingPacket::new(PayloadId::new(0, symbol_id), payload)
                    );
                } else {
//println!("SOURCE {}", symbol_id);
                    // Check that this is not an extended symbol (which aren't explicitly sent)
                    assert!(symbol_id < self.source_block_symbols);
                    // Source symbol
                    self.source_symbols[symbol_id as usize] =
                        Some(Symbol::new(payload));
                    self.received_source_symbols += 1;
//println!("RECEIVED SOURCE {}", self.received_source_symbols);
                }
            }
        }

//        let num_extended_symbols = extended_source_block_symbols(self.source_block_symbols);
        if self.received_source_symbols == self.source_block_symbols {
            let result = self
                .source_symbols
                .iter()
                .cloned()
                .map(|symbol| symbol.unwrap().into_bytes())
                .flatten()
                .collect();
//println!("DECODED!");

            self.decoded = true;
            return Some(result);
        }

        if self.received_esi.len() as u32 >= num_extended_symbols {
            let s = num_ldpc_symbols(self.source_block_symbols) as usize;
            let h = num_hdpc_symbols(self.source_block_symbols) as usize;

            let mut encoded_indices = vec![];
            // See section 5.3.3.4.2. There are S + H zero symbols to start the D vector
            let mut d = vec![Symbol::zero(self.symbol_size); s + h];
            for (i, source) in self.source_symbols.iter().enumerate() {
                if let Some(symbol) = source {
                    encoded_indices.push(i as u32);
                    d.push(symbol.clone());
                }
            }

            // Append the extended padding symbols
            for i in self.source_block_symbols..num_extended_symbols {
                encoded_indices.push(i);
                d.push(Symbol::zero(self.symbol_size));
            }

            for repair_packet in self.repair_packets.iter() {
                encoded_indices.push(repair_packet.payload_id.encoding_symbol_id());
                d.push(Symbol::new(repair_packet.data.clone()));
            }

//            if extended_source_block_symbols(self.source_block_symbols) >= self.sparse_threshold {
            if num_extended_symbols >= self.sparse_threshold {
                let (constraint_matrix, hdpc) = generate_constraint_matrix::<SparseBinaryMatrix>(
                    self.source_block_symbols,
                    &encoded_indices,
                );
                return self.try_pi_decode(constraint_matrix, hdpc, d);
            } else {
                let (constraint_matrix, hdpc) = generate_constraint_matrix::<DenseBinaryMatrix>(
                    self.source_block_symbols,
                    &encoded_indices,
                );
                return self.try_pi_decode(constraint_matrix, hdpc, d);
            }
        }
        None
    }

    fn rebuild_source_symbol(
        &self,
        intermediate_symbols: &[Symbol],
        source_symbol_id: u32,
        lt_symbols: u32,
        pi_symbols: u32,
        sys_index: u32,
        p1: u32,
    ) -> Symbol {
        let mut rebuilt = Symbol::zero(self.symbol_size);
        let tuple = intermediate_tuple(source_symbol_id, lt_symbols, sys_index, p1);

        for i in enc_indices(tuple, lt_symbols, pi_symbols, p1) {
            rebuilt += &intermediate_symbols[i];
        }
        rebuilt
    }
}

