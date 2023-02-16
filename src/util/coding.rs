use std::{intrinsics::copy_nonoverlapping, mem::size_of};

pub fn encode_fixed32(dst: &mut [u8], value: u32) {
    unsafe {
        let bytes = value.to_le_bytes();
        copy_nonoverlapping(bytes.as_ptr(), dst.as_mut_ptr(), size_of::<u32>());
    }
}

pub fn encode_fixed64(dst: &mut [u8], value: u64) {
    unsafe {
        let bytes = value.to_le_bytes();
        copy_nonoverlapping(bytes.as_ptr(), dst.as_mut_ptr(), size_of::<u64>());
    }
}

/// Return the index after encoded data
pub fn encode_varint32(dst: &mut [u8], value: u32) -> usize {
    const B: u32 = 128;
    let mut index = 0;
    if value < (1 << 7) {
        dst[index] = value as u8;
        index += 1;
    } else if value < (1 << 14) {
        dst[index] = (value | B) as u8;
        dst[index + 1] = (value >> 7) as u8;
        index += 2;
    } else if value < (1 << 21) {
        dst[index] = (value | B) as u8;
        dst[index + 1] = ((value >> 7) | B) as u8;
        dst[index + 2] = (value >> 14) as u8;
        index += 3;
    } else if value < (1 << 28) {
        dst[index] = (value | B) as u8;
        dst[index + 1] = ((value >> 7) | B) as u8;
        dst[index + 2] = ((value >> 14) | B) as u8;
        dst[index + 3] = (value >> 21) as u8;
        index += 4;
    } else {
        dst[index] = (value | B) as u8;
        dst[index + 1] = ((value >> 7) | B) as u8;
        dst[index + 2] = ((value >> 14) | B) as u8;
        dst[index + 3] = ((value >> 21) | B) as u8;
        dst[index + 4] = (value >> 28) as u8;
        index += 5;
    }
    index
}

/// Return the index after encoded data
pub fn encode_varint64(dst: &mut [u8], mut value: u64) -> usize {
    const B: u64 = 128;
    let mut index = 0;
    while value >= B {
        dst[index] = (value | B) as u8;
        value >>= 7;
        index += 1;
    }
    dst[index] = value as u8;
    index + 1
}

pub fn decode_fixed32(input: &[u8]) -> u32 {
    let (bytes, _) = input.split_at(size_of::<u32>());
    u32::from_le_bytes(bytes.try_into().unwrap())
}

pub fn decode_fixed64(input: &[u8]) -> u64 {
    let (bytes, _) = input.split_at(size_of::<u64>());
    u64::from_le_bytes(bytes.try_into().unwrap())
}

pub fn decode_varint32(input: &[u8]) -> Option<(u32, usize)> {
    let mut result = 0;
    for i in 0..input.len().min(5) {
        let byte = input[i] as u32;
        if byte & 128 != 0 {
            // More
            result |= (byte & 127) << (i * 7);
        } else {
            result |= byte << (i * 7);
            return Some((result, i + 1));
        }
    }
    None
}

pub fn decode_varint64(input: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0;
    for i in 0..input.len().min(10) {
        let byte = input[i] as u64;
        if byte & 128 != 0 {
            // More
            result |= (byte & 127) << (i * 7);
        } else {
            result |= byte << (i * 7);
            return Some((result, i + 1));
        }
    }
    None
}

pub fn extend_fixed32(dst: &mut Vec<u8>, value: u32) {
    let mut buf = [0u8; size_of::<u32>()];
    encode_fixed32(&mut buf, value);
    dst.extend_from_slice(&buf);
}

pub fn extend_fixed64(dst: &mut Vec<u8>, value: u64) {
    let mut buf = [0u8; size_of::<u64>()];
    encode_fixed64(&mut buf, value);
    dst.extend_from_slice(&buf);
}

pub fn extend_varint32(dst: &mut Vec<u8>, value: u32) {
    let mut buf = [0u8; 5];
    let len = encode_varint32(&mut buf, value);
    dst.extend_from_slice(&buf[..len]);
}

pub fn extend_varint64(dst: &mut Vec<u8>, value: u64) {
    let mut buf = [0u8; 10];
    let len = encode_varint64(&mut buf, value);
    dst.extend_from_slice(&buf[..len]);
}

pub fn extend_size_prefixed_slice(dst: &mut Vec<u8>, value: &[u8]) {
    extend_varint32(dst, value.len() as u32);
    dst.extend_from_slice(value);
}

/// Return Some(result, remain) if success
pub fn decode_size_prefixed_slice(input: &[u8]) -> Option<(&[u8], usize)> {
    let (len, offset) = decode_varint32(input)?;
    if offset + len as usize <= input.len() {
        let result = &input[offset..offset + len as usize];
        Some((result, offset + len as usize))
    } else {
        None
    }
}

/// Get byte size of a varint
pub fn varint_size(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 128 {
        value >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use std::{mem::size_of, str};

    use super::*;

    #[test]
    fn test_coding_fixed32() {
        let mut s = vec![];
        for v in 0..100000 {
            extend_fixed32(&mut s, v);
        }
        let mut i = 0;
        for v in 0..100000 {
            let actual = decode_fixed32(&s[i..i + size_of::<u32>()]);
            assert_eq!(v, actual);
            i += size_of::<u32>();
        }
    }

    #[test]
    fn test_coding_fixed64() {
        let mut s = vec![];
        for power in 0..63 {
            let v = 1u64 << power;
            extend_fixed64(&mut s, v - 1);
            extend_fixed64(&mut s, v + 0);
            extend_fixed64(&mut s, v + 1);
        }

        let mut i = 0;
        for power in 0..63 {
            let v = 1u64 << power;
            let actual = decode_fixed64(&s[i..i + size_of::<u64>()]);
            assert_eq!(v - 1, actual);
            i += size_of::<u64>();
            let actual = decode_fixed64(&s[i..i + size_of::<u64>()]);
            assert_eq!(v + 0, actual);
            i += size_of::<u64>();
            let actual = decode_fixed64(&s[i..i + size_of::<u64>()]);
            assert_eq!(v + 1, actual);
            i += size_of::<u64>();
        }
    }

    #[test]
    fn test_coding_encoding_output() {
        let mut s = vec![];
        extend_fixed32(&mut s, 0x04030201);
        assert_eq!(4, s.len());
        assert_eq!(0x01, s[0]);
        assert_eq!(0x02, s[1]);
        assert_eq!(0x03, s[2]);
        assert_eq!(0x04, s[3]);
        s.clear();
        extend_fixed64(&mut s, 0x0807060504030201);
        assert_eq!(8, s.len());
        assert_eq!(0x01, s[0]);
        assert_eq!(0x02, s[1]);
        assert_eq!(0x03, s[2]);
        assert_eq!(0x04, s[3]);
        assert_eq!(0x05, s[4]);
        assert_eq!(0x06, s[5]);
        assert_eq!(0x07, s[6]);
        assert_eq!(0x08, s[7]);
    }

    #[test]
    fn test_coding_varint32() {
        let mut s = vec![];
        for i in 0..32 * 32 {
            let v = (i / 32) << (i % 32);
            extend_varint32(&mut s, v);
        }

        let mut index = 0;
        for i in 0..32 * 32 {
            let expected = (i / 32) << (i % 32);
            let (actual, offset) = decode_varint32(&s[index..]).unwrap();
            assert_eq!(expected, actual);
            assert_eq!(varint_size(actual as u64), offset);
            index += offset;
        }
        assert_eq!(index, s.len());
    }

    #[test]
    fn test_coding_varint64() {
        // Construct the list of values to check
        let mut values = vec![];
        // Some special values
        values.push(0);
        values.push(100);
        values.push(u64::MAX);
        values.push(u64::MAX - 1);
        for k in 0..64 {
            // Test values near powers of two
            let power = 1 << k;
            values.push(power);
            values.push(power - 1);
            values.push(power + 1);
        }

        let mut s = vec![];
        for &value in &values {
            extend_varint64(&mut s, value);
        }

        let mut index = 0;
        for value in values {
            assert!(index < s.len());
            let (actual, offset) = decode_varint64(&s[index..]).unwrap();
            assert_eq!(value, actual);
            assert_eq!(varint_size(actual), offset);
            index += offset;
        }
        assert_eq!(index, s.len());
    }

    #[test]
    fn test_coding_varint32_overflow() {
        let input = [0x81, 0x82, 0x83, 0x84, 0x85, 0x11];
        assert!(decode_varint32(&input).is_none());
    }

    #[test]
    fn test_coding_varint32_truncation() {
        let large_value = (1 << 31) + 100;
        let mut s = vec![];
        extend_varint32(&mut s, large_value);
        for len in 0..s.len() - 1 {
            assert!(decode_varint32(&s[..len]).is_none());
        }
        assert_eq!(large_value, decode_varint32(&s).unwrap().0);
    }

    #[test]
    fn test_coding_varint64_overflow() {
        let input = [
            0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11,
        ];
        assert!(decode_varint64(&input).is_none())
    }

    #[test]
    fn test_coding_varint64_truncation() {
        let large_value = (1 << 63) + 100;
        let mut s = vec![];
        extend_varint64(&mut s, large_value);
        for len in 0..s.len() - 1 {
            assert!(decode_varint64(&s[..len]).is_none());
        }
        assert_eq!(large_value, decode_varint64(&s).unwrap().0);
    }

    #[test]
    fn test_coding_strings() {
        let mut s = vec![];
        extend_size_prefixed_slice(&mut s, &"".as_bytes());
        extend_size_prefixed_slice(&mut s, &"foo".as_bytes());
        extend_size_prefixed_slice(&mut s, &"bar".as_bytes());
        extend_size_prefixed_slice(&mut s, &(vec![b'x'; 200].as_slice()));

        let mut offset = 0;
        let (result, len) = decode_size_prefixed_slice(&s[offset..]).unwrap();
        offset += len;
        assert_eq!(str::from_utf8(&result).unwrap(), "");
        let (result, len) = decode_size_prefixed_slice(&s[offset..]).unwrap();
        offset += len;
        assert_eq!(str::from_utf8(&result).unwrap(), "foo");
        let (result, len) = decode_size_prefixed_slice(&s[offset..]).unwrap();
        offset += len;
        assert_eq!(str::from_utf8(&result).unwrap(), "bar");
        let (result, len) = decode_size_prefixed_slice(&s[offset..]).unwrap();
        offset += len;
        assert_eq!(
            str::from_utf8(&result).unwrap(),
            str::from_utf8(&vec![b'x'; 200]).unwrap()
        );
        assert_eq!(offset, s.len())
    }
}
