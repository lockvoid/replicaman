//! Transliterated from `Tests/ReplicaManTests/GzipTests.swift` (2 cases).
//!
//! The push body's framing: real gzip (RFC 1952 — magic bytes, inflatable by
//! any gzip reader), round-trips, and actually shrinks the JSON rows it exists
//! for.

use crate::gzip;

#[test]
fn round_trips_and_carries_gzip_magic() {
    let text = r#"{"op":"row.set","stream":"projects","data":{"name":"x"}}"#.repeat(200);
    let text = text.as_bytes();
    let zipped = gzip::compress(text).unwrap();
    assert_eq!(&zipped[..2], &[0x1f, 0x8b]);
    assert!(zipped.len() < text.len() / 5);
    assert_eq!(gzip::decompress(&zipped).unwrap(), text);

    // Empty input still produces a well-formed member, not empty bytes: a pair
    // of identity functions would satisfy the round trip alone, which is why
    // this rides the magic-byte assertion instead of standing as its own test.
    let empty = gzip::compress(&[]).unwrap();
    assert_eq!(&empty[..2], &[0x1f, 0x8b]);
    assert_eq!(gzip::decompress(&empty).unwrap(), Vec::<u8>::new());
}

#[test]
fn garbage_does_not_inflate() {
    assert!(gzip::decompress(&[1, 2, 3, 4, 5]).is_err());
}

#[test]
fn rejects_truncation_corruption_and_expansion_past_the_limit() {
    let input = vec![b'x'; 262_145];
    let zipped = gzip::compress(&input).unwrap();
    assert_eq!(
        gzip::decompress_bounded(&zipped, input.len()).unwrap(),
        input
    );
    assert!(gzip::decompress_bounded(&zipped, input.len() - 1).is_err());
    assert!(gzip::decompress(&zipped[..zipped.len() - 1]).is_err());
    let mut corrupt = zipped.clone();
    let crc_offset = corrupt.len() - 8;
    corrupt[crc_offset] ^= 1;
    assert!(gzip::decompress(&corrupt).is_err());
}
