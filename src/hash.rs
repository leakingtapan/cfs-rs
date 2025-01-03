use anyhow::Result;
use sha2::{Digest as Sha2Digest, Sha256};
use std::io::Read;

pub fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:02x}", hasher.finalize())
}

/// generate the sha256 hash from a stream of bytes
pub fn sha256_read(rdr: &mut dyn Read) -> Result<(String, usize)> {
    let mut hasher = Sha256::new();
    //TODO: larger block size?
    //let mut buff: [u8; 64] = [0; 64];
    let mut buff: [u8; 8192] = [0; 8192];
    let mut len = 0;
    loop {
        let size = rdr.read(&mut buff)?;
        if size == 0 {
            break;
        }
        len += size;
        hasher.update(&buff[0..size]);
    }
    let hash = format!("{:02x}", hasher.finalize());
    Ok((hash, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_sha256() {
        let hash = sha256(b"hello world");
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_sha256_read() {
        let mut input = Cursor::new("hello world".as_bytes());
        let actual = sha256_read(&mut input);
        let actual = actual.unwrap();
        assert_eq!(
            actual,
            (
                "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9".to_string(),
                11
            )
        );
    }
}
