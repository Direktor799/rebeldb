const MASK_DELTA: u32 = 0xa282ead8;

pub use crc32c::{crc32c, crc32c_append};

//. It is problematic to compute the CRC of a string that
//. contains embedded CRCs.  Therefore we recommend that CRCs stored
//. somewhere (e.g., in files) should be masked before being stored.
pub fn crc32c_mask(crc: u32) -> u32 {
    ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA)
}

pub fn crc32c_unmask(masked_crc: u32) -> u32 {
    let rot = masked_crc.wrapping_sub(MASK_DELTA);
    (rot >> 17) | (rot << 15)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_crc_standard_results() {
        let mut buf = [0; 32];
        assert_eq!(0x8a9136aa, crc32c(&buf));
        buf.iter_mut().for_each(|byte| *byte = 0xff);
        assert_eq!(0x62a8ab43, crc32c(&buf));
        buf.iter_mut()
            .enumerate()
            .for_each(|(index, byte)| *byte = index as u8);
        assert_eq!(0x46dd794e, crc32c(&buf));

        buf.iter_mut()
            .rev()
            .enumerate()
            .for_each(|(index, byte)| *byte = index as u8);
        assert_eq!(0x113fdb5c, crc32c(&buf));

        let data = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(0xd9963a56, crc32c(&data));
    }

    #[test]
    fn test_crc_values() {
        assert_ne!(crc32c("a".as_bytes()), crc32c("foo".as_bytes()));
    }

    #[test]
    fn test_crc_extend() {
        assert_eq!(
            crc32c("hello world".as_bytes()),
            crc32c_append(crc32c("hello ".as_bytes()), "world".as_bytes())
        );
    }

    #[test]
    fn test_crc_mask() {
        let crc = crc32c("foo".as_bytes());
        assert_ne!(crc, crc32c_mask(crc));
        assert_ne!(crc, crc32c_mask(crc32c_mask(crc)));
        assert_eq!(crc, crc32c_unmask(crc32c_mask(crc)));
        assert_eq!(
            crc,
            crc32c_unmask(crc32c_unmask(crc32c_mask(crc32c_mask(crc))))
        );
    }
}
