//! Shareable links that carry a query in the URL fragment.

use serde::{Deserialize, Serialize};

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
const MAX_QUERY_BYTES: usize = 64 * 1024;

/// A query shared through a link.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedQuery {
    /// Connection identifier.
    #[serde(rename = "c")]
    pub connection_id: String,
    /// Database.
    #[serde(rename = "d")]
    pub database: String,
    /// Query text.
    #[serde(rename = "q")]
    pub query: String,
}

/// Encodes bytes as unpadded base64url.
#[must_use]
pub fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = chunk.iter().enumerate().fold(0u32, |value, (index, byte)| {
            value | u32::from(*byte) << (16 - 8 * index)
        });
        for index in 0..=chunk.len() {
            out.push(char::from(ALPHABET[(value >> (18 - 6 * index) & 63) as usize]));
        }
    }
    out
}

/// Decodes unpadded or padded base64url.
#[must_use]
pub fn base64url_decode(text: &str) -> Option<Vec<u8>> {
    let text = text.trim_end_matches('=');
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut buffer = 0u32;
    let mut bits = 0;
    for byte in text.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            _ => return None,
        };
        buffer = buffer << 6 | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits & 0xff) as u8);
        }
    }
    Some(out)
}

/// Builds the URL fragment (without `#`) for a shared query.
#[must_use]
pub fn encode(shared: &SharedQuery) -> String {
    format!(
        "q={}",
        base64url_encode(serde_json::to_string(shared).unwrap_or_default().as_bytes())
    )
}

/// Parses a URL fragment produced by [`encode`].
#[must_use]
pub fn decode(fragment: &str) -> Option<SharedQuery> {
    let payload = fragment
        .trim_start_matches('#')
        .split('&')
        .find_map(|part| part.strip_prefix("q="))?;
    if payload.len() > MAX_QUERY_BYTES * 2 {
        return None;
    }
    let shared: SharedQuery = serde_json::from_slice(&base64url_decode(payload)?).ok()?;
    (shared.query.len() <= MAX_QUERY_BYTES && !shared.query.trim().is_empty()).then_some(shared)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64url_round_trips() {
        for text in [
            "",
            "f",
            "fo",
            "foo",
            "foob",
            "fooba",
            "foobar",
            "ünïcode ✓ | where x > 1",
        ] {
            let encoded = base64url_encode(text.as_bytes());
            assert!(!encoded.contains(['+', '/', '=']));
            assert_eq!(base64url_decode(&encoded).as_deref(), Some(text.as_bytes()));
        }
        assert_eq!(base64url_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64url_encode(b"fo"), "Zm8");
        assert_eq!(base64url_decode("Zm8="), Some(b"fo".to_vec()));
        assert_eq!(base64url_decode("**"), None);
    }

    #[test]
    fn shared_queries_round_trip() {
        let shared = SharedQuery {
            connection_id: "loadtest-emulator".to_owned(),
            database: "loadtest".to_owned(),
            query: "T | where Name == \"a&b\" | take 10".to_owned(),
        };
        let fragment = encode(&shared);
        assert!(fragment.starts_with("q="));
        assert_eq!(decode(&format!("#{fragment}")), Some(shared));
        assert_eq!(decode("#other=1"), None);
        assert_eq!(decode("#q=not-json"), None);
    }
}
