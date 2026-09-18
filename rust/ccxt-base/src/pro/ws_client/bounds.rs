//! Bounds and decoding helpers for the WebSocket transport.
//!
//! The client owns the queue lifecycle; this module only keeps the queue and
//! payload accounting rules in one place so every reader/writer path uses the
//! same limits.

use std::collections::VecDeque;
use std::io::Read;
use std::mem::size_of;

use crate::Value;

/// Maximum number of bytes accepted from one WebSocket frame before decoding.
pub(super) const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
/// Maximum number of bytes a compressed frame may expand to.
pub(super) const MAX_DECODED_BYTES: usize = 1024 * 1024;
/// Maximum number of parsed frames retained by the inbound queue.
pub(super) const INCOMING_CAPACITY: usize = 256;
/// Maximum estimated retained bytes in the inbound queue.
pub(super) const INCOMING_BYTES: usize = 4 * 1024 * 1024;
/// Maximum number of frames waiting for the writer.
pub(super) const OUTGOING_CAPACITY: usize = 64;
/// Maximum estimated bytes waiting for the writer.
pub(super) const OUTGOING_BYTES: usize = 1024 * 1024;

/// A count- and byte-bounded queue of parsed WebSocket values.
///
/// The byte count is a conservative accounting estimate of the retained
/// `Value` shape rather than the wire representation; it is not a measurement
/// of allocator usage or process RSS. A small JSON frame can retain a large
/// tree of strings, map slots, and vector capacity.
#[derive(Debug, Default)]
pub(super) struct ParsedQueue {
    queue: VecDeque<(Value, usize)>,
    bytes: usize,
}

impl ParsedQueue {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Add a parsed value if both queue bounds leave room for it.
    pub(super) fn push(&mut self, value: Value) -> Result<(), String> {
        if self.queue.len() >= INCOMING_CAPACITY {
            return Err(format!(
                "[NetworkError] incoming WebSocket queue capacity exceeded ({INCOMING_CAPACITY})"
            ));
        }
        let value_bytes = estimate_value_bytes(&value);
        if value_bytes > INCOMING_BYTES.saturating_sub(self.bytes) {
            return Err(format!(
                "[NetworkError] incoming WebSocket queue byte limit exceeded ({} bytes)",
                INCOMING_BYTES
            ));
        }
        self.bytes = self.bytes.saturating_add(value_bytes);
        self.queue.push_back((value, value_bytes));
        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<Value> {
        self.queue.pop_front().map(|(value, value_bytes)| {
            self.bytes = self.bytes.saturating_sub(value_bytes);
            value
        })
    }

    pub(super) fn clear(&mut self) {
        self.queue.clear();
        self.bytes = 0;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Estimate the memory retained by a `Value` tree.
///
/// `Value` contains the inline enum storage, while arrays/maps and strings
/// retain additional allocations. Container capacity is used instead of
/// length, and container slots include a little bookkeeping overhead. Some
/// shared `Arc` storage may therefore be counted more than once; that is
/// intentional because this is a conservative accounting bound, not an
/// allocator or process-RSS measurement.
pub(super) fn estimate_value_bytes(value: &Value) -> usize {
    let mut total = size_of::<Value>();
    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => {}
        Value::Str(string) => {
            total = total.saturating_add(string.capacity());
        }
        Value::Arr(items) => {
            total = total
                .saturating_add(size_of::<Vec<Value>>())
                .saturating_add(2 * size_of::<usize>())
                .saturating_add(items.capacity().saturating_mul(size_of::<Value>()));
            for item in items.iter() {
                total = total.saturating_add(estimate_value_bytes(item));
            }
        }
        Value::Dict(map) => {
            let slot_bytes = size_of::<String>()
                .saturating_add(size_of::<Value>())
                .saturating_add(size_of::<usize>());
            total = total
                .saturating_add(size_of::<indexmap::IndexMap<String, Value>>())
                .saturating_add(2 * size_of::<usize>())
                .saturating_add(map.capacity().saturating_mul(slot_bytes));
            for (key, item) in map.iter() {
                total = total
                    .saturating_add(key.capacity())
                    .saturating_add(estimate_value_bytes(item));
            }
        }
    }
    total
}

enum Compressed {
    Invalid,
    Decoded(Vec<u8>),
    Overflow,
}

/// Read a decompressor through a one-byte-over-limit guard.
///
/// Reading `MAX_DECODED_BYTES + 1` is important: stopping at the exact limit
/// cannot distinguish an accepted payload from one that is still expanding.
fn read_compressed<R: Read>(reader: R) -> Compressed {
    let limit = (MAX_DECODED_BYTES as u64).saturating_add(1);
    let mut limited = reader.take(limit);
    let mut decoded = Vec::new();
    match limited.read_to_end(&mut decoded) {
        Ok(_) if decoded.len() > MAX_DECODED_BYTES => Compressed::Overflow,
        Ok(_) => Compressed::Decoded(decoded),
        Err(_) if decoded.len() > MAX_DECODED_BYTES => Compressed::Overflow,
        Err(_) => Compressed::Invalid,
    }
}

fn parse_text(text: &str) -> Value {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(json) => Value::from_json(&json),
        // Bare control messages such as `pong` are not JSON strings on the
        // wire. Preserve those, and other venue-specific plain text, as text.
        Err(_) => Value::Str(text.to_owned()),
    }
}

fn parse_binary_bytes(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => parse_text(text),
        Err(_) => crate::exchange_stubs::bytes_to_value(bytes),
    }
}

/// Preserve the old decompressor behavior: only a non-empty, valid UTF-8
/// expansion is accepted as a compressed text frame. Empty or binary
/// expansions fall through to the next decoder/raw-byte path.
fn parse_compressed(bytes: &[u8]) -> Option<Value> {
    if bytes.is_empty() {
        return None;
    }
    std::str::from_utf8(bytes).ok().map(parse_text)
}

/// Decode one raw WebSocket payload under the transport bounds.
///
/// Binary payloads are tried as gzip, then raw deflate. A malformed compressed
/// stream is allowed to fall through to plain UTF-8/bytes handling; an
/// expansion overflow is terminal and never falls through to an unbounded raw
/// interpretation.
pub(super) fn decode(payload: &[u8], is_binary: bool) -> Result<Value, String> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(format!(
            "[NetworkError] WebSocket payload exceeds {MAX_PAYLOAD_BYTES} bytes"
        ));
    }

    if !is_binary {
        let text = std::str::from_utf8(payload).map_err(|error| {
            format!("[NetworkError] WebSocket text is not valid UTF-8: {error}")
        })?;
        return Ok(parse_text(text));
    }

    match read_compressed(flate2::read::GzDecoder::new(payload)) {
        Compressed::Decoded(decoded) => {
            if let Some(value) = parse_compressed(&decoded) {
                return Ok(value);
            }
        }
        Compressed::Overflow => {
            return Err(format!(
                "[NetworkError] decoded WebSocket payload exceeds {MAX_DECODED_BYTES} bytes"
            ));
        }
        Compressed::Invalid => {}
    }

    match read_compressed(flate2::read::DeflateDecoder::new(payload)) {
        Compressed::Decoded(decoded) => {
            if let Some(value) = parse_compressed(&decoded) {
                return Ok(value);
            }
        }
        Compressed::Overflow => {
            return Err(format!(
                "[NetworkError] decoded WebSocket payload exceeds {MAX_DECODED_BYTES} bytes"
            ));
        }
        Compressed::Invalid => {}
    }

    Ok(parse_binary_bytes(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn queue_enforces_count_and_accounts_bytes() {
        let mut queue = ParsedQueue::new();
        queue.push(Value::Str("hello".to_owned())).unwrap();
        assert_eq!(queue.len(), 1);
        assert!(!queue.is_empty());
        assert!(queue.bytes() >= size_of::<Value>() + 5);
        assert_eq!(queue.pop(), Some(Value::Str("hello".to_owned())));
        assert!(queue.is_empty());
        assert_eq!(queue.bytes(), 0);

        for _ in 0..INCOMING_CAPACITY {
            queue.push(Value::Null).unwrap();
        }
        assert_eq!(queue.len(), INCOMING_CAPACITY);
        assert!(queue.push(Value::Null).is_err());
        queue.clear();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.bytes(), 0);

        let too_large = Value::Str("x".repeat(INCOMING_BYTES));
        assert!(queue.push(too_large).is_err());
    }

    #[test]
    fn repeated_pushes_hit_byte_budget_before_count_budget() {
        let mut queue = ParsedQueue::new();
        let item = Value::Str("x".repeat(32 * 1024));
        while queue.push(item.clone()).is_ok() {}
        assert!(queue.len() < INCOMING_CAPACITY);
        assert!(queue.len() > 1);
        assert!(queue.bytes() <= INCOMING_BYTES);
        let len = queue.len();
        let bytes = queue.bytes();
        assert!(queue.push(item).is_err());
        assert_eq!(queue.len(), len);
        assert_eq!(queue.bytes(), bytes);
    }

    #[test]
    fn gzip_expansion_limit_is_accepted() {
        let input = vec![b'x'; MAX_DECODED_BYTES];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&input).unwrap();
        let compressed = encoder.finish().unwrap();
        let result = decode(&compressed, true).unwrap();
        assert_eq!(result.as_str().map(str::len), Some(MAX_DECODED_BYTES));
    }

    #[test]
    fn gzip_expansion_overflow_is_terminal() {
        let input = vec![b'x'; MAX_DECODED_BYTES + 1];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&input).unwrap();
        let compressed = encoder.finish().unwrap();
        let result = decode(&compressed, true);
        assert!(
            matches!(result, Err(message) if message.starts_with("[NetworkError] decoded WebSocket payload"))
        );
    }

    #[test]
    fn raw_deflate_expansion_overflow_is_terminal() {
        let input = vec![b'x'; MAX_DECODED_BYTES + 1];
        let mut encoder =
            flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&input).unwrap();
        let compressed = encoder.finish().unwrap();
        let result = decode(&compressed, true);
        assert!(
            matches!(result, Err(message) if message.starts_with("[NetworkError] decoded WebSocket payload"))
        );
    }

    #[test]
    fn invalid_utf8_text_is_rejected() {
        assert!(decode(&[0xff, 0xfe], false).is_err());
    }

    #[test]
    fn invalid_utf8_binary_is_retained_as_bytes() {
        let payload = [0x00, 0xff, 0x01];
        assert_eq!(
            decode(&payload, true).unwrap(),
            crate::exchange_stubs::bytes_to_value(&payload)
        );
    }

    #[test]
    fn raw_json_binary_matches_text() {
        let payload = br#"{"channel":"ticker","value":1}"#;
        assert_eq!(decode(payload, true), decode(payload, false));
    }

    #[test]
    fn empty_compressed_expansion_falls_back_to_nonempty_raw_payload() {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let compressed = encoder.finish().unwrap();
        assert!(!compressed.is_empty());
        assert_eq!(
            decode(&compressed, true).unwrap(),
            crate::exchange_stubs::bytes_to_value(&compressed)
        );
    }

    #[test]
    fn zero_length_payload_is_safe() {
        assert_eq!(decode(&[], true).unwrap(), Value::Str(String::new()));
        assert_eq!(decode(&[], false).unwrap(), Value::Str(String::new()));
    }

    #[test]
    fn bare_pong_is_plain_text() {
        assert_eq!(
            decode(b"pong", false).unwrap(),
            Value::Str("pong".to_owned())
        );
    }
}
