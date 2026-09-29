//! Gzip framing with explicit errors and a bounded decoded body.

use std::io::{self, Read, Write};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

pub fn compress(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    encoder.finish()
}

pub fn decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    decompress_bounded(data, crate::protocol::RESPONSE_BYTES)
}

pub fn decompress_bounded(data: &[u8], limit: usize) -> io::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut output = Vec::new();
    let mut chunk = [0_u8; 8192];

    loop {
        let count = decoder.read(&mut chunk)?;
        if count == 0 {
            return Ok(output);
        }
        if count > limit.saturating_sub(output.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "gzip output exceeds size limit",
            ));
        }
        output.extend_from_slice(&chunk[..count]);
    }
}
