use crate::gf2::add_assign_binary;
use crate::iterators::OctetIter;
use crate::octet::Octet;
use crate::util::get_both_indices;
use serde::{Deserialize, Serialize};
use std::mem::size_of;

// TODO: change this struct to not use the Octet class, since it's binary not GF(256)
pub trait BinaryMatrix: Clone {
    fn new(height: usize, width: usize, trailing_dense_column_hint: usize) -> Self;

    fn set(&mut self, i: usize, j: usize, value: Octet);

    fn height(&self) -> usize;

    fn width(&self) -> usize;

    fn size_in_bytes(&self) -> usize;

    fn count_ones(&self, row: usize, start_col: usize, end_col: usize) -> usize;

    // Once "impl Trait" is supported in traits, it would be better to return "impl Iterator<...>"
    fn get_row_iter(&self, row: usize, start_col: usize, end_col: usize) -> OctetIter;

    // An iterator over rows with a 1-valued entry for the given col
    fn get_ones_in_column(&self, col: usize, start_row: usize, end_row: usize) -> Vec<u32>;

    // Get a slice of columns from a row as Octets
    fn get_sub_row_as_octets(&self, row: usize, start_col: usize) -> Vec<u8>;

    fn get(&self, i: usize, j: usize) -> Octet;

    fn swap_rows(&mut self, i: usize, j: usize);

    // start_row_hint indicates that all preceding rows don't need to be swapped, because they have
    // identical values
    fn swap_columns(&mut self, i: usize, j: usize, start_row_hint: usize);

    fn enable_column_acccess_acceleration(&mut self);

    // After calling this method swap_columns() and other column oriented methods, may be much slower
    fn disable_column_acccess_acceleration(&mut self);

    // Hints that column i will not be swapped again, and is likely to become dense'ish
    fn hint_column_dense_and_frozen(&mut self, i: usize);

    // other must be a rows x rows matrix
    // sets self[0..rows][..] = X * self[0..rows][..]
    fn mul_assign_submatrix(&mut self, other: &Self, rows: usize);

    fn add_assign_rows(&mut self, dest: usize, src: usize);

    fn resize(&mut self, new_height: usize, new_width: usize);
}

const WORD_WIDTH: usize = 64;

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Hash)]
pub struct DenseBinaryMatrix {
    height: usize,
    width: usize,
    // Values are bit-packed into u64
    // TODO: optimize into a single dimensional vec
    elements: Vec<Vec<u64>>,
}

impl DenseBinaryMatrix {
    // Returns (word in elements vec, and bit in word) for the given col
    pub fn bit_position(col: usize) -> (usize, usize) {
        return (col / WORD_WIDTH, col % WORD_WIDTH);
    }

    // Returns mask to select the given bit in a word
    pub fn select_mask(bit: usize) -> u64 {
        1u64 << (bit as u64)
    }

    // Select the bit and all bits to the left
    fn select_bit_and_all_left_mask(bit: usize) -> u64 {
        !DenseBinaryMatrix::select_all_right_of_mask(bit)
    }

    // Select all bits right of the given bit
    fn select_all_right_of_mask(bit: usize) -> u64 {
        let mask = DenseBinaryMatrix::select_mask(bit);
        // Subtract one to convert e.g. 0100 -> 0011
        mask - 1
    }

    fn clear_bit(word: &mut u64, bit: usize) {
        *word &= !DenseBinaryMatrix::select_mask(bit);
    }

    fn set_bit(word: &mut u64, bit: usize) {
        *word |= DenseBinaryMatrix::select_mask(bit);
    }
}

impl BinaryMatrix for DenseBinaryMatrix {
    fn new(height: usize, width: usize, _: usize) -> DenseBinaryMatrix {
        let elements = vec![vec![0; DenseBinaryMatrix::bit_position(width).0 + 1]; height];
        DenseBinaryMatrix {
            height,
            width,
            elements,
        }
    }

    fn set(&mut self, i: usize, j: usize, value: Octet) {
        let (word, bit) = DenseBinaryMatrix::bit_position(j);
        if value == Octet::zero() {
            DenseBinaryMatrix::clear_bit(&mut self.elements[i][word], bit);
        } else {
            DenseBinaryMatrix::set_bit(&mut self.elements[i][word], bit);
        }
    }

    fn height(&self) -> usize {
        self.height
    }

    fn width(&self) -> usize {
        self.width
    }

    fn size_in_bytes(&self) -> usize {
        let mut bytes = size_of::<Self>();
        bytes += size_of::<Vec<u64>>() * self.elements.len();
        bytes += size_of::<u64>() * self.height * self.width;

        bytes
    }

    fn count_ones(&self, row: usize, start_col: usize, end_col: usize) -> usize {
        let (start_word, start_bit) = DenseBinaryMatrix::bit_position(start_col);
        let (end_word, end_bit) = DenseBinaryMatrix::bit_position(end_col);
        // Handle case when there is only one word
        if start_word == end_word {
            let mut mask = DenseBinaryMatrix::select_bit_and_all_left_mask(start_bit);
            mask &= DenseBinaryMatrix::select_all_right_of_mask(end_bit);
            let bits = self.elements[row][start_word] & mask;
            return bits.count_ones() as usize;
        }

        let first_word_bits = self.elements[row][start_word]
            & DenseBinaryMatrix::select_bit_and_all_left_mask(start_bit);
        let mut ones = first_word_bits.count_ones();
        for word in (start_word + 1)..end_word {
            ones += self.elements[row][word].count_ones();
        }
        if end_bit > 0 {
            let bits =
                self.elements[row][end_word] & DenseBinaryMatrix::select_all_right_of_mask(end_bit);
            ones += bits.count_ones();
        }

        return ones as usize;
    }

    fn get_row_iter(&self, row: usize, start_col: usize, end_col: usize) -> OctetIter {
        OctetIter::new_dense_binary(start_col, end_col, &self.elements[row])
    }

    fn get_ones_in_column(&self, col: usize, start_row: usize, end_row: usize) -> Vec<u32> {
        let mut rows = vec![];
        for row in start_row..end_row {
            if self.get(row, col) == Octet::one() {
                rows.push(row as u32);
            }
        }

        rows
    }

    fn get_sub_row_as_octets(&self, row: usize, start_col: usize) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.width - start_col);
        for col in start_col..self.width {
            result.push(self.get(row, col).byte());
        }

        result
    }

    fn get(&self, i: usize, j: usize) -> Octet {
        let (word, bit) = DenseBinaryMatrix::bit_position(j);
        if self.elements[i][word] & DenseBinaryMatrix::select_mask(bit) == 0 {
            return Octet::zero();
        } else {
            return Octet::one();
        }
    }

    fn swap_rows(&mut self, i: usize, j: usize) {
        self.elements.swap(i, j);
    }

    fn swap_columns(&mut self, i: usize, j: usize, start_row_hint: usize) {
        for row in start_row_hint..self.elements.len() {
            let value_i = self.get(row, i);
            let value_j = self.get(row, j);
            self.set(row, i, value_j);
            self.set(row, j, value_i);
        }
    }

    fn enable_column_acccess_acceleration(&mut self) {
        // No-op
    }

    fn disable_column_acccess_acceleration(&mut self) {
        // No-op
    }

    fn hint_column_dense_and_frozen(&mut self, _: usize) {
        // No-op
    }

    // other must be a rows x rows matrix
    // sets self[0..rows][..] = X * self[0..rows][..]
    fn mul_assign_submatrix(&mut self, other: &DenseBinaryMatrix, rows: usize) {
        assert_eq!(rows, other.height());
        assert_eq!(rows, other.width());
        assert!(rows <= self.height());
        let mut temp = vec![vec![0; DenseBinaryMatrix::bit_position(self.width).0 + 1]; rows];
        #[allow(clippy::needless_range_loop)]
        for row in 0..rows {
            for i in 0..rows {
                let scalar = other.get(row, i);
                if scalar == Octet::zero() {
                    continue;
                }
                add_assign_binary(&mut temp[row], &self.elements[i]);
            }
        }
        for row in (0..rows).rev() {
            self.elements[row] = temp.pop().unwrap();
        }
    }

    fn add_assign_rows(&mut self, dest: usize, src: usize) {
        assert_ne!(dest, src);
        let (dest_row, temp_row) = get_both_indices(&mut self.elements, dest, src);
        add_assign_binary(dest_row, temp_row);
    }

    fn resize(&mut self, new_height: usize, new_width: usize) {
        assert!(new_height <= self.height);
        assert!(new_width <= self.width);
        let (new_words, _) = DenseBinaryMatrix::bit_position(new_width);
        self.elements.truncate(new_height);
        for row in 0..self.elements.len() {
            self.elements[row].truncate(new_words + 1);
        }
        self.height = new_height;
        self.width = new_width;
    }
}

