use crate::octet::Octet;
use crate::octets::add_assign;
use crate::octets::fused_addassign_mul_scalar;
use crate::octets::mulassign_scalar;
use serde::{Deserialize, Serialize};
use std::ops::AddAssign;

/// Elementary unit of data, for encoding/decoding purposes.
#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Hash)]
pub struct Symbol {
    value: Vec<u8>,
}

impl Symbol {
    pub fn new(value: Vec<u8>) -> Symbol {
        Symbol { value }
    }

    /// Initialize a zeroed symbol, with given size.
    pub fn zero<T>(size: T) -> Symbol
    where
        T: Into<usize>,
    {
        Symbol {
            value: vec![0; size.into()],
        }
    }

    #[cfg(feature = "benchmarking")]
    pub fn len(&self) -> usize {
        self.value.len()
    }

    /// Return the underlying byte slice for a symbol.
    pub fn as_bytes(&self) -> &[u8] {
        &self.value
    }

    /// Consume a symbol into a vector of bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.value
    }

    pub fn mulassign_scalar(&mut self, scalar: &Octet) {
        mulassign_scalar(&mut self.value, scalar);
    }

    pub fn fused_addassign_mul_scalar(&mut self, other: &Symbol, scalar: &Octet) {
        fused_addassign_mul_scalar(&mut self.value, &other.value, scalar);
    }
}

impl<'a> AddAssign<&'a Symbol> for Symbol {
    fn add_assign(&mut self, other: &'a Symbol) {
        add_assign(&mut self.value, &other.value);
    }
}

