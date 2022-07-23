use std::{cell::RefCell, ops::Range, rc::Rc, slice::from_raw_parts};

use super::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::{
    env::SequentialFile,
    util::{crc32c, crc32c_unmask, decode_fixed32, DBError},
};

#[derive(Debug)]
#[repr(u8)]
enum ExtendRecordType {
    Eof,
    BadRecord,
}

pub struct Reader {
    file: Rc<RefCell<dyn SequentialFile>>,
    reporter: Option<Rc<RefCell<dyn Reporter>>>,
    checksum: bool,
    backing_store: [u8; BLOCK_SIZE],
    scratch: Vec<u8>,
    buffer_range: Range<usize>,
    eof: bool,
    /// Offset of the last record returned by ReadRecord.
    last_record_offset: usize,
    /// Offset of the first location past the end of buffer.
    end_of_buffer_offset: usize,
    initial_offset: usize,
    /// True if we are resynchronizing after a seek (initial_offset > 0). In
    /// particular, a run of kMiddleType and kLastType records can be silently
    /// skipped in this mode
    resyncing: bool,
}

impl Reader {
    pub fn new(
        file: Rc<RefCell<dyn SequentialFile>>,
        initial_offset: usize,
        checksum: bool,
        reporter: Option<Rc<RefCell<dyn Reporter>>>,
    ) -> Self {
        Self {
            file,
            reporter,
            checksum,
            backing_store: [0; BLOCK_SIZE],
            scratch: vec![],
            buffer_range: 0..0,
            eof: false,
            last_record_offset: 0,
            end_of_buffer_offset: 0,
            initial_offset,
            resyncing: initial_offset > 0,
        }
    }

    pub fn read_record(&mut self) -> Option<&[u8]> {
        if self.last_record_offset < self.initial_offset {
            if !self.skip_to_initial_block() {
                return None;
            }
        }

        self.scratch.clear();
        let mut in_fragmented_record = false;
        let mut prospective_record_offset = 0;

        loop {
            let result = self.read_physical_record();
            let fragment = result.1;
            // ReadPhysicalRecord may have only had an empty trailer remaining in its
            // internal buffer. Calculate the offset of the next physical record now
            // that it has returned, properly accounting for its header size.
            let physical_record_offset = self.end_of_buffer_offset as isize
                - self.buffer_range.len() as isize
                - HEADER_SIZE as isize
                - fragment.len() as isize;
            if let Ok(record_type) = result.0 {
                if self.resyncing {
                    match record_type {
                        RecordType::Middle => continue,
                        RecordType::Last => {
                            self.resyncing = false;
                            continue;
                        }
                        _ => self.resyncing = false,
                    }
                }
            }

            match result.0 {
                Ok(RecordType::Full) => {
                    if in_fragmented_record && !self.scratch.is_empty() {
                        self.report_corruption(self.scratch.len(), "partial record without end(1)");
                    }
                    assert!(physical_record_offset >= 0);
                    prospective_record_offset = physical_record_offset as usize;
                    self.scratch.clear();
                    self.last_record_offset = prospective_record_offset;
                    return Some(&fragment);
                }
                Ok(RecordType::First) => {
                    if in_fragmented_record && !self.scratch.is_empty() {
                        self.report_corruption(self.scratch.len(), "partial record without end(2)");
                    }
                    assert!(physical_record_offset >= 0);
                    prospective_record_offset = physical_record_offset as usize;
                    self.scratch = fragment.to_vec();
                    in_fragmented_record = true;
                }
                Ok(RecordType::Middle) => {
                    if !in_fragmented_record {
                        self.report_corruption(
                            fragment.len(),
                            "missing start of fragmented record(1)",
                        )
                    } else {
                        self.scratch.extend_from_slice(fragment);
                    }
                }
                Ok(RecordType::Last) => {
                    if !in_fragmented_record {
                        self.report_corruption(
                            fragment.len(),
                            "missing start of fragmented record(2)",
                        )
                    } else {
                        self.scratch.extend_from_slice(fragment);
                        self.last_record_offset = prospective_record_offset;
                        return Some(&self.scratch);
                    }
                }
                Err(ExtendRecordType::Eof) => {
                    if in_fragmented_record {
                        // This can be caused by the writer dying immediately after
                        // writing a physical record but before completing the next; don't
                        // treat it as a corruption, just ignore the entire logical record.
                        self.scratch.clear();
                    }
                    return None;
                }
                Err(ExtendRecordType::BadRecord) => {
                    if in_fragmented_record {
                        self.report_corruption(self.scratch.len(), "error in middle of record");
                        in_fragmented_record = false;
                        self.scratch.clear();
                    }
                }
                record_type => {
                    let drop_size = if in_fragmented_record {
                        self.scratch.len() + fragment.len()
                    } else {
                        fragment.len()
                    };
                    self.report_corruption(
                        drop_size,
                        &format!("unknown record type {:?}", record_type),
                    );
                    in_fragmented_record = false;
                    self.scratch.clear();
                }
            }
        }
    }

    pub fn last_record_offset(&self) -> usize {
        self.last_record_offset
    }

    /// Skips all blocks that are completely before "initial_offset".
    /// Returns true on success. Handles reporting.
    fn skip_to_initial_block(&mut self) -> bool {
        let offset_in_block = self.initial_offset % BLOCK_SIZE;
        let mut block_start_location = self.initial_offset - offset_in_block;
        // Don't search a block if we'd be in the trailer
        if offset_in_block > BLOCK_SIZE - 6 {
            block_start_location += BLOCK_SIZE;
        }
        self.end_of_buffer_offset = block_start_location;
        // Skip to start of first block that can contain the initial record
        if block_start_location > 0 {
            let result = self.file.borrow_mut().skip(block_start_location);
            if let Err(error) = result {
                self.report_drop(block_start_location, &error);
                return false;
            }
        }
        true
    }

    fn read_physical_record<'a>(&mut self) -> (Result<RecordType, ExtendRecordType>, &'a [u8]) {
        loop {
            if self.buffer_range.len() < HEADER_SIZE {
                if !self.eof {
                    // Last read was a full read, so this is a trailer to skip
                    self.buffer_range = 0..0;
                    let result = self.file.borrow_mut().read(&mut self.backing_store);
                    let read_size = *result.as_ref().unwrap_or(&0);
                    self.buffer_range = 0..read_size;
                    self.end_of_buffer_offset += self.buffer_range.len();
                    if result.is_err() {
                        self.buffer_range = 0..0;
                        self.report_drop(BLOCK_SIZE, &result.unwrap_err());
                        self.eof = true;
                        return (Err(ExtendRecordType::Eof), &[]);
                    } else if self.buffer_range.len() < BLOCK_SIZE {
                        self.eof = true;
                    }
                    continue;
                } else {
                    // Note that if buffer_ is non-empty, we have a truncated header at the
                    // end of the file, which can be caused by the writer crashing in the
                    // middle of writing the header. Instead of considering this an error,
                    // just report EOF.
                    self.buffer_range = 0..0;
                    return (Err(ExtendRecordType::Eof), &[]);
                }
            }

            // Parse the header
            let buffer = unsafe {
                from_raw_parts(
                    self.backing_store.as_ptr().add(self.buffer_range.start),
                    self.buffer_range.len(),
                )
            };
            let a = buffer[4] as u32;
            let b = buffer[5] as u32;
            let length = a | (b << 8);
            let type_: RecordType = buffer[6].into();
            if HEADER_SIZE + length as usize > buffer.len() {
                let drop_size = buffer.len();
                self.buffer_range = 0..0;
                if !self.eof {
                    self.report_corruption(drop_size, "bad record length");
                    return (Err(ExtendRecordType::BadRecord), &[]);
                }
                // If the end of the file has been reached without reading |length| bytes
                // of payload, assume the writer died in the middle of writing the record.
                // Don't report a corruption.
                return (Err(ExtendRecordType::Eof), &[]);
            }

            if type_ == RecordType::Zero && length == 0 {
                // Skip zero length record without reporting any drops since
                // such records are produced by the mmap based writing code in
                // env_posix.cc that preallocates file regions.
                self.buffer_range = 0..0;
                return (Err(ExtendRecordType::BadRecord), &[]);
            }

            if self.checksum {
                let expected_crc = crc32c_unmask(decode_fixed32(&buffer));
                let actual_crc = crc32c(&buffer[6..6 + 1 + length as usize]);
                if actual_crc != expected_crc {
                    // Drop the rest of the buffer since "length" itself may have
                    // been corrupted and if we trust it, we could find some
                    // fragment of a real log record that just happens to look
                    // like a valid log record.
                    let drop_size = buffer.len();
                    self.buffer_range = 0..0;
                    self.report_corruption(drop_size, "checksum mismatch");
                    return (Err(ExtendRecordType::BadRecord), &[]);
                }
            }
            self.buffer_range =
                self.buffer_range.start + HEADER_SIZE + length as usize..self.buffer_range.end;

            // Skip physical record that started before initial_offset_
            if self.end_of_buffer_offset - self.buffer_range.len() - HEADER_SIZE - (length as usize)
                < self.initial_offset
            {
                return (Err(ExtendRecordType::BadRecord), &[]);
            }
            return (
                Ok(type_),
                &buffer[HEADER_SIZE..HEADER_SIZE + length as usize],
            );
        }
    }

    fn report_corruption(&mut self, bytes: usize, msg: &str) {
        self.report_drop(bytes, &DBError::corruption(msg))
    }

    fn report_drop(&mut self, bytes: usize, reason: &DBError) {
        if self.reporter.is_some()
            && self
                .end_of_buffer_offset
                .wrapping_sub(self.buffer_range.len() + bytes)
                >= self.initial_offset
        {
            self.reporter
                .as_mut()
                .unwrap()
                .borrow_mut()
                .corruption(bytes, reason)
        }
    }
}

pub trait Reporter {
    fn corruption(&mut self, bytes: usize, error: &DBError);
}
