use std::error::Error;

use hex::{decode, encode};

pub fn hex(bytes: &[u8]) -> String {
    encode(bytes)
}

pub fn to_bytes(hex: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    Ok(decode(hex)?)
}

#[cfg(test)]
mod test {
    use crate::hex::{hex, to_bytes};

    #[test]
    fn test_hex() {
        assert_eq!(&hex(b"\0"), "00");
        assert_eq!(&hex(b"\x0a"), "0a");
        assert_eq!(&hex(b"\x00\x0a\x00\x00\x00\x0a\x00"), "000a0000000a00");
        assert_eq!(&hex(b"\0\x01\x0a"), "00010a");
    }

    #[test]
    fn hex_and_back() {
        assert_eq!(&hex(&to_bytes("0a").unwrap()), "0a");
        assert_eq!(&hex(&to_bytes("abc123def0").unwrap()), "abc123def0");
        assert_eq!(
            &hex(&to_bytes(&hex(
                &to_bytes("a0ef2d9bb064800d8faceb96832b3ed26eb57412").unwrap()
            ))
            .unwrap()),
            "a0ef2d9bb064800d8faceb96832b3ed26eb57412"
        );
    }
}
