// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! FSST (Fast Static Symbol Table) string compression.
//!
//! FSST compresses short strings by replacing frequently occurring substrings
//! (up to [`FSST_MAX_SYMBOL_LEN`] bytes) with single-byte codes drawn from a
//! small static symbol table. Because the table is static for a block of
//! values, individual compressed strings can be decompressed independently,
//! which supports random access into the encoded data.
//!
//! A compressed stream is a sequence of 1-byte codes. A code in `0..n_symbols`
//! expands to the symbol with that index; the reserved code [`FSST_ESCAPE`]
//! (`255`) indicates that the following byte is an uncompressed literal. The
//! symbol table is therefore limited to [`FSST_MAX_SYMBOLS`] entries.
//!
//! The symbol table is built by [`SymbolTable::train`], which implements the
//! generational construction from the [FSST paper] §4.3: starting from an empty
//! table, it repeatedly compresses a sample, counts how often each symbol and
//! each pair of adjacent symbols occurs, and rebuilds the table from the
//! highest-gain symbols (and concatenations of adjacent pairs).
//!
//! [FSST paper]: https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf

use std::collections::HashMap;

use crate::errors::{ParquetError, Result};

/// Escape code: the byte immediately following it is an uncompressed literal.
pub(crate) const FSST_ESCAPE: u8 = 255;

/// Maximum number of symbols in a table. Codes `0..=254` address symbols;
/// code `255` ([`FSST_ESCAPE`]) is reserved for escaped literals.
pub(crate) const FSST_MAX_SYMBOLS: usize = 255;

/// Maximum length, in bytes, of a single symbol.
pub(crate) const FSST_MAX_SYMBOL_LEN: usize = 8;

/// Number of passes used to grow the symbol table during training.
const TRAINING_GENERATIONS: usize = 5;

/// Cap on the number of sample bytes inspected per training generation, to keep
/// training time bounded on large pages.
const TRAINING_SAMPLE_BUDGET: usize = 1 << 20;

/// Pack up to 8 bytes into a `u64` key (little-endian, zero-padded).
fn pack(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() <= 8);
    let mut buf = [0u8; 8];
    buf[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(buf)
}

/// A static symbol table mapping 1-byte codes to byte strings.
#[derive(Debug, Clone)]
pub(crate) struct SymbolTable {
    /// `symbols[code]` is the byte string that `code` expands to on decode.
    symbols: Vec<Vec<u8>>,
    /// Lookup acceleration for compression: `lookup[len]` maps the packed bytes
    /// of every symbol of that length to its code. Index `0` is unused.
    lookup: Vec<HashMap<u64, u8>>,
}

impl Default for SymbolTable {
    fn default() -> Self {
        Self::with_symbols(Vec::new())
    }
}

impl SymbolTable {
    /// Build a table from an explicit symbol list, computing the compression
    /// lookup index. The list must contain at most [`FSST_MAX_SYMBOLS`] symbols,
    /// each at most [`FSST_MAX_SYMBOL_LEN`] bytes long.
    fn with_symbols(symbols: Vec<Vec<u8>>) -> Self {
        debug_assert!(symbols.len() <= FSST_MAX_SYMBOLS);
        let mut lookup: Vec<HashMap<u64, u8>> = vec![HashMap::new(); FSST_MAX_SYMBOL_LEN + 1];
        for (idx, symbol) in symbols.iter().enumerate() {
            debug_assert!((1..=FSST_MAX_SYMBOL_LEN).contains(&symbol.len()));
            lookup[symbol.len()].insert(pack(symbol), idx as u8);
        }
        Self { symbols, lookup }
    }

    /// Train a symbol table over a set of sample values.
    ///
    /// Implements the generational algorithm from the [FSST paper] §4.3.
    ///
    /// [FSST paper]: https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf
    pub(crate) fn train<'a>(samples: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let lines: Vec<&[u8]> = samples
            .into_iter()
            .filter(|line| !line.is_empty())
            .collect();
        if lines.is_empty() {
            return Self::default();
        }

        let mut symbols: Vec<Vec<u8>> = Vec::new();
        for _ in 0..TRAINING_GENERATIONS {
            let table = Self::with_symbols(symbols.clone());

            // Count single-symbol and adjacent-symbol-pair occurrences over the
            // sample, compressed with the current table.
            let mut count1: HashMap<u16, u64> = HashMap::new();
            let mut count2: HashMap<(u16, u16), u64> = HashMap::new();
            let mut budget = TRAINING_SAMPLE_BUDGET;
            for line in &lines {
                if budget == 0 {
                    break;
                }
                budget = budget.saturating_sub(line.len());

                let mut prev: Option<u16> = None;
                let mut i = 0;
                while i < line.len() {
                    let (code, len) = table.longest_code(&line[i..]);
                    *count1.entry(code).or_default() += 1;
                    if let Some(prev) = prev {
                        *count2.entry((prev, code)).or_default() += 1;
                    }
                    prev = Some(code);
                    i += len;
                }
            }

            symbols = Self::select_symbols(&symbols, &count1, &count2);
        }

        Self::with_symbols(symbols)
    }

    /// Build the next generation of symbols by ranking candidates by gain.
    ///
    /// Candidates are every symbol seen during counting (gain = frequency ×
    /// length, with an 8× boost for single bytes, per the paper's heuristic) and
    /// every concatenation of an adjacent symbol pair that fits in
    /// [`FSST_MAX_SYMBOL_LEN`] bytes (gain = pair frequency × concatenated
    /// length). The [`FSST_MAX_SYMBOLS`] highest-gain candidates are kept.
    fn select_symbols(
        current: &[Vec<u8>],
        count1: &HashMap<u16, u64>,
        count2: &HashMap<(u16, u16), u64>,
    ) -> Vec<Vec<u8>> {
        // A `code` is the byte value `0..256` (a literal) or `256 + symbol_index`.
        let code_bytes = |code: u16| -> Vec<u8> {
            if (code as usize) < 256 {
                vec![code as u8]
            } else {
                current[code as usize - 256].clone()
            }
        };

        let mut candidates: HashMap<Vec<u8>, u64> = HashMap::new();
        for (&code, &freq) in count1 {
            let bytes = code_bytes(code);
            let mut gain = freq * bytes.len() as u64;
            if bytes.len() == 1 {
                gain *= 8;
            }
            *candidates.entry(bytes).or_default() += gain;
        }
        for (&(prev, code), &freq) in count2 {
            let mut bytes = code_bytes(prev);
            bytes.extend_from_slice(&code_bytes(code));
            if bytes.len() <= FSST_MAX_SYMBOL_LEN {
                let gain = freq * bytes.len() as u64;
                *candidates.entry(bytes).or_default() += gain;
            }
        }

        let mut ranked: Vec<(Vec<u8>, u64)> = candidates.into_iter().collect();
        // Highest gain first; break ties toward longer symbols for determinism.
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.len().cmp(&a.0.len())));
        ranked.truncate(FSST_MAX_SYMBOLS);
        ranked.into_iter().map(|(bytes, _)| bytes).collect()
    }

    /// Find the longest symbol that is a prefix of `input`, returning its code
    /// (`< 256` for a single-byte literal, `256 + index` for a table symbol) and
    /// the number of input bytes it consumes. Used during training.
    fn longest_code(&self, input: &[u8]) -> (u16, usize) {
        match self.longest_symbol(input) {
            Some((idx, len)) => (256 + idx as u16, len),
            None => (input[0] as u16, 1),
        }
    }

    /// Find the longest table symbol that is a prefix of `input`, returning its
    /// index and length, or `None` if no symbol matches.
    fn longest_symbol(&self, input: &[u8]) -> Option<(u8, usize)> {
        let max = input.len().min(FSST_MAX_SYMBOL_LEN);
        for len in (1..=max).rev() {
            if let Some(&idx) = self.lookup[len].get(&pack(&input[..len])) {
                return Some((idx, len));
            }
        }
        None
    }

    /// Compress `input`, appending the encoded bytes to `out`.
    pub(crate) fn compress(&self, input: &[u8], out: &mut Vec<u8>) {
        let mut i = 0;
        while i < input.len() {
            match self.longest_symbol(&input[i..]) {
                Some((idx, len)) => {
                    out.push(idx);
                    i += len;
                }
                None => {
                    out.push(FSST_ESCAPE);
                    out.push(input[i]);
                    i += 1;
                }
            }
        }
    }

    /// Decompress `input`, appending the decoded bytes to `out`.
    pub(crate) fn decompress(&self, input: &[u8], out: &mut Vec<u8>) -> Result<()> {
        let mut i = 0;
        while i < input.len() {
            let code = input[i];
            i += 1;
            if code == FSST_ESCAPE {
                let lit = *input
                    .get(i)
                    .ok_or_else(|| general_err!("FSST: dangling escape at end of stream"))?;
                out.push(lit);
                i += 1;
            } else {
                let symbol = self
                    .symbols
                    .get(code as usize)
                    .ok_or_else(|| general_err!("FSST: code {} not in symbol table", code))?;
                out.extend_from_slice(symbol);
            }
        }
        Ok(())
    }

    /// Serialize the table as a header that precedes the compressed data.
    ///
    /// Layout: `num_symbols: u8`, then for each symbol `len: u8` followed by
    /// `len` bytes.
    pub(crate) fn serialize(&self, out: &mut Vec<u8>) {
        debug_assert!(self.symbols.len() <= FSST_MAX_SYMBOLS);
        out.push(self.symbols.len() as u8);
        for symbol in &self.symbols {
            debug_assert!(symbol.len() <= FSST_MAX_SYMBOL_LEN);
            out.push(symbol.len() as u8);
            out.extend_from_slice(symbol);
        }
    }

    /// Deserialize a table from the front of `data`, returning the table and the
    /// number of bytes consumed.
    pub(crate) fn deserialize(data: &[u8]) -> Result<(Self, usize)> {
        let mut i = 0;
        let count = *data
            .get(i)
            .ok_or_else(|| general_err!("FSST: missing symbol-table header"))? as usize;
        i += 1;
        let mut symbols = Vec::with_capacity(count);
        for _ in 0..count {
            let len = *data
                .get(i)
                .ok_or_else(|| general_err!("FSST: truncated symbol length"))? as usize;
            i += 1;
            let end = i + len;
            let symbol = data
                .get(i..end)
                .ok_or_else(|| general_err!("FSST: truncated symbol data"))?
                .to_vec();
            symbols.push(symbol);
            i = end;
        }
        Ok((Self::with_symbols(symbols), i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(table: &SymbolTable, input: &[u8]) -> Vec<u8> {
        let mut compressed = Vec::new();
        table.compress(input, &mut compressed);
        let mut decompressed = Vec::new();
        table.decompress(&compressed, &mut decompressed).unwrap();
        assert_eq!(decompressed, input, "roundtrip mismatch");
        compressed
    }

    #[test]
    fn empty_table_roundtrips_everything() {
        let table = SymbolTable::train(std::iter::empty());
        let inputs: &[&[u8]] = &[b"", b"hello", b"parquet", &[0u8, 255, 1, 254]];
        for input in inputs {
            roundtrip(&table, input);
        }
    }

    #[test]
    fn trained_table_compresses_repetitive_data() {
        let corpus: Vec<&[u8]> = (0..256).map(|_| b"http://example.com/".as_slice()).collect();
        let table = SymbolTable::train(corpus.iter().copied());

        let input = b"http://example.com/path";
        let compressed = roundtrip(&table, input);
        assert!(
            compressed.len() < input.len(),
            "expected compression, got {} >= {}",
            compressed.len(),
            input.len()
        );
    }

    #[test]
    fn trained_table_roundtrips_unseen_bytes() {
        // Bytes never seen in training must still round-trip via escapes.
        let table = SymbolTable::train([b"aaaaaaaa".as_slice(), b"bbbbbbbb".as_slice()]);
        roundtrip(&table, &[0u8, 1, 2, 250, 255]);
        roundtrip(&table, b"aaaabbbb\x00\xff");
    }

    #[test]
    fn serialize_roundtrip_preserves_decoding() {
        let table = SymbolTable::train([b"the quick brown fox".as_slice()]);
        let input = b"the fox";
        let mut compressed = Vec::new();
        table.compress(input, &mut compressed);

        let mut header = Vec::new();
        table.serialize(&mut header);
        let (restored, consumed) = SymbolTable::deserialize(&header).unwrap();
        assert_eq!(consumed, header.len());

        let mut decompressed = Vec::new();
        restored.decompress(&compressed, &mut decompressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn decompress_rejects_dangling_escape() {
        let table = SymbolTable::default();
        let mut out = Vec::new();
        assert!(table.decompress(&[FSST_ESCAPE], &mut out).is_err());
    }
}
