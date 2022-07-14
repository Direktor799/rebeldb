const MASK_DELTA: u32 = 0xa282ead8;

pub fn extend(init_crc: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(init_crc, data)
}

pub fn value(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

//. It is problematic to compute the CRC of a string that
//. contains embedded CRCs.  Therefore we recommend that CRCs stored
//. somewhere (e.g., in files) should be masked before being stored.
pub fn mask(crc: u32) -> u32 {
    ((crc >> 15) | (crc << 17)).wrapping_add(MASK_DELTA)
}

pub fn unmask(masked_crc: u32) -> u32 {
    let rot = masked_crc.wrapping_sub(MASK_DELTA);
    (rot >> 17) | (rot << 15)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_crc_standard_results() {
        let mut buf = [0; 32];
        assert_eq!(0x8a9136aa, value(&buf));
        buf.iter_mut().for_each(|byte| *byte = 0xff);
        assert_eq!(0x62a8ab43, value(&buf));
        buf.iter_mut()
            .enumerate()
            .for_each(|(index, byte)| *byte = index as u8);
        assert_eq!(0x46dd794e, value(&buf));

        buf.iter_mut()
            .rev()
            .enumerate()
            .for_each(|(index, byte)| *byte = index as u8);
        assert_eq!(0x113fdb5c, value(&buf));

        let data = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(0xd9963a56, value(&data));
    }

    #[test]
    fn test_crc_values() {
        assert_ne!(value("a".as_bytes()), value("foo".as_bytes()));
    }

    #[test]
    fn test_crc_extend() {
        assert_eq!(
            value("hello world".as_bytes()),
            extend(value("hello ".as_bytes()), "world".as_bytes())
        );
    }

    #[test]
    fn test_crc_mask() {
        let crc = value("foo".as_bytes());
        assert_ne!(crc, mask(crc));
        assert_ne!(crc, mask(mask(crc)));
        assert_eq!(crc, unmask(mask(crc)));
        assert_eq!(crc, unmask(unmask(mask(mask(crc)))));
    }
}
