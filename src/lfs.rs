use anyhow::Result;
use std::io::Read;

#[derive(PartialEq, Debug)]
pub struct LfsFile {
    pub hash: String,
    pub hash_alg: String,
    pub size: i64,
}

impl LfsFile {
    pub fn new(rdr: &mut dyn Read) -> Result<LfsFile> {
        let mut buff: [u8; 256] = [0; 256];
        rdr.read(&mut buff)?;
        let lfs_file_header = "version https://git-lfs.github.com/spec/v1";

        let file_content = String::from_utf8_lossy(&buff);
        let lines: Vec<_> = file_content
            .trim_matches(char::from(0))
            .split("\n")
            .collect();

        if lines[0] != lfs_file_header {
            return Err(anyhow::Error::msg("invalid pointer file header"));
        }

        let oid_line: Vec<_> = lines[1].split(" ").collect();
        if oid_line[0] != "oid" {
            return Err(anyhow::Error::msg("invalid LFS oid"));
        }

        let alg_hash: Vec<_> = oid_line[1].split(":").collect();
        let size_line: Vec<_> = lines[2].split(" ").collect();
        let size = size_line[1].parse::<i64>().unwrap();

        Ok(LfsFile {
            hash: alg_hash[1].to_string(),
            hash_alg: alg_hash[0].to_string(),
            size: size,
        })
    }
}

mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_valid_lfs_file() {
        let valid_lfs_file = "version https://git-lfs.github.com/spec/v1
oid sha256:9e3454192f3b84dc8d40d92d4e89ca24800c9618324e87ce08ba5b0ea30364ea
size 51052147";
        let mut input = Cursor::new(valid_lfs_file.as_bytes());

        let expected = LfsFile {
            hash: String::from("9e3454192f3b84dc8d40d92d4e89ca24800c9618324e87ce08ba5b0ea30364ea"),
            hash_alg: String::from("sha256"),
            size: 51052147,
        };
        //assert_eq!(true, is_lfs_file(&mut input).unwrap());
        assert_eq!(expected, LfsFile::new(&mut input).unwrap());
    }

    #[test]
    fn test_invalid_lfs_file() {
        let invalid_lfs_file = "invalid";

        let mut input = Cursor::new(invalid_lfs_file.as_bytes());
        assert_eq!(true, LfsFile::new(&mut input).is_err());
    }
}
