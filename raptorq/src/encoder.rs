use crate::base::intermediate_tuple;
use crate::base::partition;
use crate::base::EncodingPacket;
use crate::base::PayloadId;
use crate::constraint_matrix::generate_constraint_matrix;
use crate::matrix::DenseBinaryMatrix;
use crate::operation_vector::{perform_op, SymbolOps};
use crate::pi_solver::fused_inverse_mul_symbols;
use crate::sparse_matrix::SparseBinaryMatrix;
use crate::symbol::Symbol;
use crate::systematic_constants::extended_source_block_symbols;
use crate::systematic_constants::num_hdpc_symbols;
use crate::systematic_constants::num_intermediate_symbols;
use crate::systematic_constants::num_ldpc_symbols;
use crate::systematic_constants::num_lt_symbols;
use crate::systematic_constants::num_pi_symbols;
use crate::systematic_constants::{calculate_p1, systematic_index};
use crate::ObjectTransmissionInformation;
use serde::{Deserialize, Serialize};

pub const SPARSE_MATRIX_THRESHOLD: u32 = 250;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Encoder {
    config: ObjectTransmissionInformation,
    blocks: Vec<SourceBlockEncoder>,
}

impl Encoder {
    pub fn with_defaults(data: &[u8], maximum_transmission_unit: u16) -> Encoder {
        let config = ObjectTransmissionInformation::with_defaults(
            data.len() as u64,
            maximum_transmission_unit,
        );

        let kt = (config.transfer_length() as f64 / config.symbol_size() as f64).ceil() as u32;
        let (kl, ks, zl, zs) = partition(kt, config.source_blocks());

        // TODO: support subblocks
        assert_eq!(1, config.sub_blocks());
        //        let (tl, ts, nl, ns) = partition((config.symbol_size() / config.alignment() as u16) as u32, config.sub_blocks());

        let mut data_index = 0;
        let mut blocks = vec![];
        if zl > 0 {
            let kl_plan = SourceBlockEncodingPlan::generate(kl as u16);
            for i in 0..zl {
                let offset = kl as usize * config.symbol_size() as usize;
                blocks.push(SourceBlockEncoder::with_encoding_plan(
                    i as u8,
                    config.symbol_size(),
                    &data[data_index..(data_index + offset)],
                    &kl_plan,
                ));
                data_index += offset;
            }
        }

        if zs > 0 {
            let ks_plan = SourceBlockEncodingPlan::generate(ks as u16);
            for i in 0..zs {
                let offset = ks as usize * config.symbol_size() as usize;
                if data_index + offset <= data.len() {
                    blocks.push(SourceBlockEncoder::with_encoding_plan(
                        i as u8,
                        config.symbol_size(),
                        &data[data_index..(data_index + offset)],
                        &ks_plan,
                    ));
                } else {
                    // Should only be possible when Kt * T > F. See third to last paragraph in section 4.4.1.2
                    assert!(kt as usize * config.symbol_size() as usize > data.len());
                    // Zero pad the last symbol
                    let mut padded = Vec::from(&data[data_index..]);
                    padded.extend(vec![
                        0;
                        kt as usize * config.symbol_size() as usize - data.len()
                    ]);
                    blocks.push(SourceBlockEncoder::new(
                        i as u8,
                        config.symbol_size(),
                        &padded,
                    ));
                }
                data_index += offset;
            }
        }

        Encoder { config, blocks }
    }

    pub fn get_config(&self) -> ObjectTransmissionInformation {
        self.config.clone()
    }

    pub fn get_encoded_packets(&self, repair_packets_per_block: u32) -> Vec<EncodingPacket> {
        let mut packets = vec![];
        for encoder in self.blocks.iter() {
            packets.extend(encoder.source_packets());
            packets.extend(encoder.repair_packets(0, repair_packets_per_block));
        }
        packets
    }

    pub fn get_block_encoders(&self) -> &Vec<SourceBlockEncoder> {
        &self.blocks
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceBlockEncodingPlan {
    operations: Vec<SymbolOps>,
    source_symbol_count: u16,
}

impl SourceBlockEncodingPlan {
    // Generates an encoding plan that is valid for any combination of data length and symbol size
    // where ceil(data_length / symbol_size) = symbol_count
    pub fn generate(symbol_count: u16) -> SourceBlockEncodingPlan {
        // TODO: refactor pi_solver, so that we don't need this dummy data to generate a plan
        let symbols = vec![Symbol::new(vec![0]); symbol_count as usize];
        let (_, ops) = gen_intermediate_symbols(&symbols, 1, SPARSE_MATRIX_THRESHOLD);
        SourceBlockEncodingPlan {
            operations: ops.unwrap(),
            source_symbol_count: symbol_count,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceBlockEncoder {
    source_block_id: u8,
    source_symbols: Vec<Symbol>,
    intermediate_symbols: Vec<Symbol>,
}

impl SourceBlockEncoder {
    pub fn new(source_block_id: u8, symbol_size: u16, data: &[u8]) -> SourceBlockEncoder {
        assert_eq!(data.len() % symbol_size as usize, 0);
        let source_symbols: Vec<Symbol> = data
            .chunks(symbol_size as usize)
            .map(|x| Symbol::new(Vec::from(x)))
            .collect();

        let (intermediate_symbols, _) = gen_intermediate_symbols(
            &source_symbols,
            symbol_size as usize,
            SPARSE_MATRIX_THRESHOLD,
        );

        SourceBlockEncoder {
            source_block_id,
            source_symbols,
            intermediate_symbols: intermediate_symbols.unwrap(),
        }
    }

    pub fn with_encoding_plan(
        source_block_id: u8,
        symbol_size: u16,
        data: &[u8],
        plan: &SourceBlockEncodingPlan,
    ) -> SourceBlockEncoder {
        assert_eq!(data.len() % symbol_size as usize, 0);
        let source_symbols: Vec<Symbol> = data
            .chunks(symbol_size as usize)
            .map(|x| Symbol::new(Vec::from(x)))
            .collect();
        // TODO: this could be more lenient and support anything with the same extended symbol count
        assert_eq!(source_symbols.len(), plan.source_symbol_count as usize);

        let intermediate_symbols = gen_intermediate_symbols_with_plan(
            &source_symbols,
            symbol_size as usize,
            &plan.operations,
        );

        SourceBlockEncoder {
            source_block_id,
            source_symbols,
            intermediate_symbols,
        }
    }

    pub fn source_packets(&self) -> Vec<EncodingPacket> {
        let mut esi: i32 = -1;
        self.source_symbols
            .iter()
            .map(|symbol| {
                esi += 1;
                EncodingPacket::new(
                    PayloadId::new(self.source_block_id, esi as u32),
                    symbol.as_bytes().to_vec(),
                )
            })
            .collect()
    }

    // See section 5.3.4
    pub fn repair_packets(&self, start_repair_symbol_id: u32, packets: u32) -> Vec<EncodingPacket> {
        let source_symbols = self.source_symbols.len() as u32;
        let start_encoding_symbol_id = start_repair_symbol_id - source_symbols 
            + extended_source_block_symbols(source_symbols);
        let mut result = vec![];
        let lt_symbols = num_lt_symbols(source_symbols);
        let pi_symbols = num_pi_symbols(source_symbols);
        let sys_index = systematic_index(source_symbols as u32);
        let p1 = calculate_p1(source_symbols, pi_symbols);
        for i in 0..packets {
            let tuple = intermediate_tuple(start_encoding_symbol_id + i, lt_symbols, sys_index, p1);
            result.push(EncodingPacket::new(
                PayloadId::new(self.source_block_id, start_repair_symbol_id + i),
                enc(
                    self.source_symbols.len() as u32,
                    &self.intermediate_symbols,
                    tuple,
                )
                .into_bytes(),
            ));
        }
        result
    }
}

#[allow(non_snake_case)]
fn create_d(
    source_block: &[Symbol],
    symbol_size: usize,
    extended_source_symbols: usize,
) -> Vec<Symbol> {
    let L = num_intermediate_symbols(source_block.len() as u32);
    let S = num_ldpc_symbols(source_block.len() as u32);
    let H = num_hdpc_symbols(source_block.len() as u32);

    let mut D = Vec::with_capacity(L as usize);
    for _ in 0..(S + H) {
        D.push(Symbol::zero(symbol_size));
    }
    for symbol in source_block {
        D.push(symbol.clone());
    }
    // Extend the source block with padding. See section 5.3.2
    for _ in 0..(extended_source_symbols as usize - source_block.len()) {
        D.push(Symbol::zero(symbol_size));
    }
    assert_eq!(D.len(), L as usize);
    D
}

// See section 5.3.3.4
#[allow(non_snake_case)]
fn gen_intermediate_symbols(
    source_block: &[Symbol],
    symbol_size: usize,
    sparse_threshold: u32,
) -> (Option<Vec<Symbol>>, Option<Vec<SymbolOps>>) {
    let extended_source_symbols = extended_source_block_symbols(source_block.len() as u32);
    let D = create_d(source_block, symbol_size, extended_source_symbols as usize);

    let indices: Vec<u32> = (0..extended_source_symbols).collect();
    if extended_source_symbols >= sparse_threshold {
        let (A, hdpc) =
            generate_constraint_matrix::<SparseBinaryMatrix>(extended_source_symbols, &indices);
        return fused_inverse_mul_symbols(A, hdpc, D, extended_source_symbols);
    } else {
        let (A, hdpc) =
            generate_constraint_matrix::<DenseBinaryMatrix>(extended_source_symbols, &indices);
        return fused_inverse_mul_symbols(A, hdpc, D, extended_source_symbols);
    }
}

#[allow(non_snake_case)]
fn gen_intermediate_symbols_with_plan(
    source_block: &[Symbol],
    symbol_size: usize,
    operation_vector: &[SymbolOps],
) -> Vec<Symbol> {
    let extended_source_symbols = extended_source_block_symbols(source_block.len() as u32);
    let mut D = create_d(source_block, symbol_size, extended_source_symbols as usize);

    for op in operation_vector {
        perform_op(op, &mut D);
    }
    D
}

// Enc[] function, as defined in section 5.3.5.3
#[allow(clippy::many_single_char_names)]
fn enc(
    source_block_symbols: u32,
    intermediate_symbols: &[Symbol],
    source_tuple: (u32, u32, u32, u32, u32, u32),
) -> Symbol {
    let w = num_lt_symbols(source_block_symbols);
    let p = num_pi_symbols(source_block_symbols);
    let p1 = calculate_p1(source_block_symbols, p);
    let (d, a, mut b, d1, a1, mut b1) = source_tuple;

    assert!(1 <= a && a < w);
    assert!(b < w);
    assert!(d1 == 2 || d1 == 3);
    assert!(1 <= a1 && a < w);
    assert!(b1 < w);

    let mut result = intermediate_symbols[b as usize].clone();
    for _ in 1..d {
        b = (b + a) % w;
        result += &intermediate_symbols[b as usize];
    }

    while b1 >= p {
        b1 = (b1 + a1) % p1;
    }

    result += &intermediate_symbols[(w + b1) as usize];

    for _ in 1..d1 {
        b1 = (b1 + a1) % p1;
        while b1 >= p {
            b1 = (b1 + a1) % p1;
        }
        result += &intermediate_symbols[(w + b1) as usize];
    }

    result
}

