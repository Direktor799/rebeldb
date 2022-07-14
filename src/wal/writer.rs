use super::{RecordType, BLOCK_SIZE, MAX_RECORD_TYPE};
use crate::{
    env::WritableFile,
    util::{crc32c, crc32c_append, crc32c_mask, encode_fixed32, Result},
    wal::HEADER_SIZE,
};

struct Writer {
    dest: Box<dyn WritableFile>,
    block_offset: usize,
    /// crc32c values for all supported record types.  These are
    /// pre-computed to reduce the overhead of computing the crc of the
    /// record type stored in the header.
    type_crc: [u32; MAX_RECORD_TYPE as usize + 1],
}

impl Writer {
    pub fn new(dest: Box<dyn WritableFile>) -> Self {
        Self::new_at(dest, 0)
    }

    pub fn new_at(dest: Box<dyn WritableFile>, dest_len: usize) -> Self {
        let mut result = Self {
            dest,
            block_offset: dest_len % BLOCK_SIZE,
            type_crc: [0; MAX_RECORD_TYPE as usize + 1],
        };
        for (index, num) in result.type_crc.iter_mut().enumerate() {
            *num = crc32c(&[index as u8])
        }
        result
    }

    pub fn add_record(&mut self, slice: &[u8]) -> Result<()> {
        let mut slice_left = slice;
        let mut begin = true;
        loop {
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < HEADER_SIZE {
                // Switch to a new block
                if leftover > 0 {
                    // Fill the trailer (literal below relies on kHeaderSize being 7)
                    let _ = self.dest.append(&[0; 7][0..leftover]);
                }
                self.block_offset = 0;
            }
            let avial = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_length = slice_left.len().min(avial);
            let end = fragment_length == slice_left.len();
            let type_ = if begin && end {
                RecordType::Full
            } else if begin {
                RecordType::First
            } else if end {
                RecordType::Last
            } else {
                RecordType::Middle
            };
            self.emit_physical_record(type_, &slice_left[..fragment_length])?;
            slice_left = &slice_left[fragment_length..];
            if slice_left.is_empty() {
                break Ok(());
            }
            begin = false;
        }
    }

    fn emit_physical_record(&mut self, type_: RecordType, data: &[u8]) -> Result<()> {
        assert!(data.len() <= 0xffff);
        assert!(self.block_offset + HEADER_SIZE + data.len() <= BLOCK_SIZE);
        let mut buf = [0; HEADER_SIZE];
        let crc = crc32c_append(self.type_crc[type_ as usize], data);
        encode_fixed32(&mut buf[0..4], crc32c_mask(crc));
        buf[4] = data.len() as u8;
        buf[5] = (data.len() >> 8) as u8;
        buf[6] = type_ as u8;
        let mut result = self.dest.append(&buf);
        if result.is_ok() {
            result = self.dest.append(data);
            if result.is_ok() {
                result = self.dest.flush();
            }
        }
        self.block_offset += HEADER_SIZE + data.len();
        result
    }
}
