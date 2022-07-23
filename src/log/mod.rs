mod reader;
mod writer;

pub use reader::{Reader, Reporter};
pub use writer::Writer;

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(u8)]
enum RecordType {
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
    Unknown = 5,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Zero,
            1 => Self::Full,
            2 => Self::First,
            3 => Self::Middle,
            4 => Self::Last,
            _ => Self::Unknown,
        }
    }
}

const MAX_RECORD_TYPE: RecordType = RecordType::Last;

const BLOCK_SIZE: usize = 32768;

/// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const HEADER_SIZE: usize = 4 + 2 + 1;

#[cfg(test)]
mod tests {
    use crate::{
        env::{SequentialFile, WritableFile},
        util::{crc32c, crc32c_mask, encode_fixed32, DBError, Random, Result},
    };
    use std::{
        cell::RefCell,
        mem::{size_of, size_of_val},
        rc::Rc,
    };

    use super::{reader::Reporter, *};

    fn big_string(partial_string: &[u8], n: usize) -> Vec<u8> {
        partial_string.iter().cycle().take(n).cloned().collect()
    }

    fn number_string(n: u32) -> Vec<u8> {
        format!("{n}.").as_bytes().to_vec()
    }

    fn random_skewed_string(i: u32, rnd: Rc<RefCell<Random>>) -> Vec<u8> {
        big_string(&number_string(i), rnd.borrow_mut().skewed(17) as usize)
    }

    const INITIAL_OFFSET_RECORD_SIZES: [usize; 6] = [
        10000, // Two sizable records in first block
        10000,
        2 * BLOCK_SIZE - 1000, // Span three blocks
        1,
        13716,                    // Consume all but two bytes of block 3.
        BLOCK_SIZE - HEADER_SIZE, // Consume the entirety of block 4.
    ];

    const INITIAL_OFFSET_LAST_RECORD_OFFSETS: [usize; 6] = [
        0,
        HEADER_SIZE + 10000,
        2 * (HEADER_SIZE + 10000),
        2 * (HEADER_SIZE + 10000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE,
        2 * (HEADER_SIZE + 10000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE + HEADER_SIZE + 1,
        3 * BLOCK_SIZE,
    ];

    const NUM_INITIAL_OFFSET_RECORDS: usize = INITIAL_OFFSET_LAST_RECORD_OFFSETS.len();

    struct LogTest {
        dest: Rc<RefCell<StringDest>>,
        source: Rc<RefCell<StringSource>>,
        reporter: Rc<RefCell<ReportCollector>>,
        reading: bool,
        writer: Box<Writer>,
        reader: Box<Reader>,
    }

    impl LogTest {
        fn new() -> Self {
            let dest = Rc::new(RefCell::new(StringDest::new()));
            let source = Rc::new(RefCell::new(StringSource::new()));
            let reporter = Rc::new(RefCell::new(ReportCollector::new()));
            Self {
                dest: dest.clone(),
                source: source.clone(),
                reporter: reporter.clone(),
                reading: false,
                writer: Box::new(Writer::new(dest)),
                reader: Box::new(Reader::new(source, 0, true, Some(reporter))),
            }
        }

        fn reopen_for_append(&mut self) {
            self.writer = Box::new(Writer::new_at(
                self.dest.clone(),
                self.dest.borrow().contents.len(),
            ));
        }

        fn write(&mut self, msg: &[u8]) {
            assert!(!self.reading, "Write() after starting to read");
            self.writer.add_record(msg).unwrap();
        }

        fn written_bytes(&self) -> usize {
            self.dest.borrow().contents.len()
        }

        fn read(&mut self) -> Vec<u8> {
            if !self.reading {
                self.reading = true;
                self.source.borrow_mut().contents = self.dest.borrow().contents.clone();
            }
            if let Some(record) = self.reader.read_record() {
                record.to_vec()
            } else {
                "EOF".as_bytes().to_vec()
            }
        }

        fn increment_byte(&mut self, offset: usize, delta: u8) {
            let contents = &mut self.dest.borrow_mut().contents;
            contents[offset] = contents[offset].wrapping_add(delta);
        }

        fn set_byte(&mut self, offset: usize, new_byte: u8) {
            self.dest.borrow_mut().contents[offset] = new_byte;
        }

        fn shrink_size(&mut self, bytes: usize) {
            let mut dest = self.dest.borrow_mut();
            let len = dest.contents.len();
            dest.contents.resize(len - bytes, 0);
        }

        fn fix_checksum(&mut self, header_offset: usize, len: usize) {
            let range = header_offset + 6..header_offset + 6 + 1 + len;
            let crc = crc32c(&self.dest.borrow().contents[range]);
            let crc = crc32c_mask(crc);
            encode_fixed32(&mut self.dest.borrow_mut().contents[header_offset..], crc);
        }

        fn force_error(&mut self) {
            self.source.borrow_mut().force_error = true;
        }

        fn dropped_bytes(&self) -> usize {
            self.reporter.borrow().dropped_bytes
        }

        fn report_message(&self) -> String {
            self.reporter.borrow().message.to_string()
        }

        /// Returns OK iff recorded error message contains "msg"
        fn match_error(&self, msg: &str) -> String {
            let reporter_inner = self.reporter.borrow();
            if reporter_inner.message.contains(msg) {
                String::from("OK")
            } else {
                reporter_inner.message.clone()
            }
        }

        fn write_initial_offset_log(&mut self) {
            for i in 0..NUM_INITIAL_OFFSET_RECORDS {
                let record = vec![b'a' + i as u8; INITIAL_OFFSET_RECORD_SIZES[i]];
                self.write(&record);
            }
        }

        fn start_reading_at(&mut self, initial_offset: usize) {
            self.reader = Box::new(Reader::new(
                self.source.clone(),
                initial_offset,
                true,
                Some(self.reporter.clone()),
            ));
        }

        fn check_offset_past_end_returns_no_records(&mut self, offset_past_end: usize) {
            self.write_initial_offset_log();
            self.reading = true;
            self.source.borrow_mut().contents = self.dest.borrow().contents.clone();
            let mut offset_reader = Reader::new(
                self.source.clone(),
                self.written_bytes() + offset_past_end,
                true,
                Some(self.reporter.clone()),
            );
            assert!(offset_reader.read_record().is_none());
        }

        fn check_initial_offset_record(
            &mut self,
            initial_offset: usize,
            expected_record_offset: usize,
        ) {
            self.write_initial_offset_log();
            self.source.borrow_mut().contents = self.dest.borrow().contents.clone();
            let mut offset_reader = Reader::new(
                self.source.clone(),
                initial_offset,
                true,
                Some(self.reporter.clone()),
            );
            // Read all records from expected_record_offset through the last one.
            assert!(expected_record_offset < NUM_INITIAL_OFFSET_RECORDS);
            for current_record_offset in expected_record_offset..NUM_INITIAL_OFFSET_RECORDS {
                let record = offset_reader.read_record().unwrap().to_vec();
                assert_eq!(
                    INITIAL_OFFSET_RECORD_SIZES[current_record_offset],
                    record.len()
                );
                assert_eq!(
                    INITIAL_OFFSET_LAST_RECORD_OFFSETS[current_record_offset],
                    offset_reader.last_record_offset()
                );
                assert_eq!(b'a' + current_record_offset as u8, record[0]);
            }
        }
    }

    struct StringDest {
        contents: Vec<u8>,
    }

    impl StringDest {
        fn new() -> Self {
            Self { contents: vec![] }
        }
    }

    impl WritableFile for StringDest {
        fn append(&mut self, data: &[u8]) -> crate::util::Result<()> {
            self.contents.extend_from_slice(data);
            Ok(())
        }

        fn close(&mut self) -> crate::util::Result<()> {
            Ok(())
        }

        fn flush(&mut self) -> crate::util::Result<()> {
            Ok(())
        }

        fn sync(&mut self) -> crate::util::Result<()> {
            Ok(())
        }
    }

    struct StringSource {
        contents: Vec<u8>,
        force_error: bool,
        returned_partial: bool,
    }

    impl StringSource {
        fn new() -> Self {
            Self {
                contents: vec![],
                force_error: false,
                returned_partial: false,
            }
        }
    }

    impl SequentialFile for StringSource {
        fn read(&mut self, dst: &mut [u8]) -> Result<usize> {
            assert!(!self.returned_partial, "must not Read() after eof/error");
            if self.force_error {
                self.force_error = false;
                self.returned_partial = true;
                return Err(DBError::corruption("read error"));
            }
            let read_size = if self.contents.len() < dst.len() {
                self.returned_partial = true;
                self.contents.len()
            } else {
                dst.len()
            };
            dst[..read_size].copy_from_slice(&self.contents[..read_size]);
            self.contents = self.contents[read_size..].to_vec();
            Ok(read_size)
        }

        fn skip(&mut self, n: usize) -> Result<()> {
            if n > self.contents.len() {
                self.contents.clear();
                Err(DBError::not_found("in-memory file skipped past end"))
            } else {
                self.contents = self.contents[n..].to_vec();
                Ok(())
            }
        }
    }

    struct ReportCollector {
        dropped_bytes: usize,
        message: String,
    }

    impl ReportCollector {
        fn new() -> Self {
            Self {
                dropped_bytes: 0,
                message: String::new(),
            }
        }
    }

    impl Reporter for ReportCollector {
        fn corruption(&mut self, bytes: usize, error: &crate::util::DBError) {
            self.dropped_bytes += bytes;
            self.message.push_str(&error.to_string());
        }
    }

    #[test]
    fn test_log_empty() {
        let mut log_test = LogTest::new();
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_read_write() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.write("bar".as_bytes());
        log_test.write("".as_bytes());
        log_test.write("xxxx".as_bytes());
        assert_eq!("foo".as_bytes(), log_test.read());
        assert_eq!("bar".as_bytes(), log_test.read());
        assert_eq!("".as_bytes(), log_test.read());
        assert_eq!("xxxx".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read()); // Make sure reads at eof work
    }

    #[test]
    fn test_log_many_blocks() {
        let mut log_test = LogTest::new();
        for i in 0..100000 {
            log_test.write(&number_string(i));
        }
        for i in 0..100000 {
            assert_eq!(number_string(i), log_test.read());
        }
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_fragmentation() {
        let mut log_test = LogTest::new();
        log_test.write("small".as_bytes());
        log_test.write(&big_string("medium".as_bytes(), 50000));
        log_test.write(&big_string("large".as_bytes(), 100000));
        assert_eq!("small".as_bytes(), log_test.read());
        assert_eq!(big_string("medium".as_bytes(), 50000), log_test.read());
        assert_eq!(big_string("large".as_bytes(), 100000), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_marginal_trailer1() {
        // Make a trailer that is exactly the same length as an empty record.
        const N: usize = BLOCK_SIZE - 2 * HEADER_SIZE;
        let mut log_test = LogTest::new();
        log_test.write(&big_string("foo".as_bytes(), N));
        assert_eq!(BLOCK_SIZE - HEADER_SIZE, log_test.written_bytes());
        log_test.write("".as_bytes());
        log_test.write("bar".as_bytes());
        assert_eq!(big_string("foo".as_bytes(), N), log_test.read());
        assert_eq!("".as_bytes(), log_test.read());
        assert_eq!("bar".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_marginal_trailer2() {
        // Make a trailer that is exactly the same length as an empty record.
        const N: usize = BLOCK_SIZE - 2 * HEADER_SIZE;
        let mut log_test = LogTest::new();
        log_test.write(&big_string("foo".as_bytes(), N));
        assert_eq!(BLOCK_SIZE - HEADER_SIZE, log_test.written_bytes());
        log_test.write("bar".as_bytes());
        assert_eq!(big_string("foo".as_bytes(), N), log_test.read());
        assert_eq!("bar".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(0, log_test.dropped_bytes());
        assert_eq!("", log_test.report_message());
    }

    #[test]
    fn test_log_short_trailer() {
        const N: usize = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        let mut log_test = LogTest::new();
        log_test.write(&big_string("foo".as_bytes(), N));
        assert_eq!(BLOCK_SIZE - HEADER_SIZE + 4, log_test.written_bytes());
        log_test.write("".as_bytes());
        log_test.write("bar".as_bytes());
        assert_eq!(big_string("foo".as_bytes(), N), log_test.read());
        assert_eq!("".as_bytes(), log_test.read());
        assert_eq!("bar".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_aligned_eof() {
        const N: usize = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        let mut log_test = LogTest::new();
        log_test.write(&big_string("foo".as_bytes(), N));
        assert_eq!(BLOCK_SIZE - HEADER_SIZE + 4, log_test.written_bytes());
        assert_eq!(big_string("foo".as_bytes(), N), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_open_for_append() {
        let mut log_test = LogTest::new();
        log_test.write("hello".as_bytes());
        log_test.reopen_for_append();
        log_test.write("world".as_bytes());
        assert_eq!("hello".as_bytes(), log_test.read());
        assert_eq!("world".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_random_read() {
        const N: u32 = 500;
        let mut log_test = LogTest::new();
        let write_rnd = Rc::new(RefCell::new(Random::new(301)));
        for i in 0..N {
            log_test.write(&random_skewed_string(i, write_rnd.clone()));
        }
        let read_rnd = Rc::new(RefCell::new(Random::new(301)));
        for i in 0..N {
            assert_eq!(random_skewed_string(i, read_rnd.clone()), log_test.read());
        }
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_read_error() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.force_error();
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(BLOCK_SIZE, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("read error"));
    }

    #[test]
    fn test_log_bad_record_type() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        // Type is stored in header[6]
        log_test.increment_byte(6, 100);
        log_test.fix_checksum(0, 3);
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(3, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("unknown record type"));
    }

    #[test]
    fn test_log_truncated_trailing_record_is_ignored() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.shrink_size(4); // Drop all payload as well as a header byte
        assert_eq!("EOF".as_bytes(), log_test.read());
        // Truncated last record is ignored, not treated as an error.
        assert_eq!(0, log_test.dropped_bytes());
        assert_eq!("", log_test.report_message());
    }

    #[test]
    fn test_log_bad_length() {
        const PAYLOAD_SIZE: usize = BLOCK_SIZE - HEADER_SIZE;
        let mut log_test = LogTest::new();
        log_test.write(&big_string("bar".as_bytes(), PAYLOAD_SIZE));
        log_test.write("foo".as_bytes());
        // Least significant size byte is stored in header[4].
        log_test.increment_byte(4, 1);
        assert_eq!("foo".as_bytes(), log_test.read());
        assert_eq!(BLOCK_SIZE, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("bad record length"));
    }

    #[test]
    fn test_log_bad_length_at_end_is_ignored() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.shrink_size(1);
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(0, log_test.dropped_bytes());
        assert_eq!("", log_test.report_message());
    }

    #[test]
    fn test_log_checksum_mismatch() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.increment_byte(0, 10);
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(10, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("checksum mismatch"));
    }

    #[test]
    fn test_log_unexpected_middle_type() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.set_byte(6, RecordType::Middle as u8);
        log_test.fix_checksum(0, 3);
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(3, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("missing start"));
    }

    #[test]
    fn test_log_unexpected_last_type() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.set_byte(6, RecordType::Last as u8);
        log_test.fix_checksum(0, 3);
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(3, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("missing start"));
    }

    #[test]
    fn test_log_unexpected_full_type() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.write("bar".as_bytes());
        log_test.set_byte(6, RecordType::First as u8);
        log_test.fix_checksum(0, 3);
        assert_eq!("bar".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(3, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("partial record without end"));
    }

    #[test]
    fn test_log_unexpected_first_type() {
        let mut log_test = LogTest::new();
        log_test.write("foo".as_bytes());
        log_test.write(&big_string("bar".as_bytes(), 100000));
        log_test.set_byte(6, RecordType::First as u8);
        log_test.fix_checksum(0, 3);
        assert_eq!(big_string("bar".as_bytes(), 100000), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(3, log_test.dropped_bytes());
        assert_eq!("OK", log_test.match_error("partial record without end"));
    }

    #[test]
    fn test_log_missing_last_is_ignored() {
        let mut log_test = LogTest::new();
        log_test.write(&big_string("bar".as_bytes(), BLOCK_SIZE));
        // Remove the LAST block, including header.
        log_test.shrink_size(14);
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(0, log_test.dropped_bytes());
        assert_eq!("", log_test.report_message());
    }

    #[test]
    fn test_log_partial_last_is_ignored() {
        let mut log_test = LogTest::new();
        log_test.write(&big_string("bar".as_bytes(), BLOCK_SIZE));
        // Cause a bad record length in the LAST block.
        log_test.shrink_size(1);
        assert_eq!("EOF".as_bytes(), log_test.read());
        assert_eq!(0, log_test.dropped_bytes());
        assert_eq!("", log_test.report_message());
    }

    #[test]
    fn test_log_skip_into_multi_record() {
        // Consider a fragmented record:
        //    first(R1), middle(R1), last(R1), first(R2)
        // If initial_offset points to a record after first(R1) but before first(R2)
        // incomplete fragment errors are not actual errors, and must be suppressed
        // until a new first or full record is encountered.
        let mut log_test = LogTest::new();
        log_test.write(&big_string("foo".as_bytes(), 3 * BLOCK_SIZE));
        log_test.write("correct".as_bytes());
        log_test.start_reading_at(BLOCK_SIZE);
        assert_eq!("correct".as_bytes(), log_test.read());
        assert_eq!(0, log_test.dropped_bytes());
        assert_eq!("", log_test.report_message());
        assert_eq!("EOF".as_bytes(), log_test.read());
    }

    #[test]
    fn test_log_error_joins_records() {
        // Consider two fragmented records:
        //    first(R1) last(R1) first(R2) last(R2)
        // where the middle two fragments disappear.  We do not want
        // first(R1),last(R2) to get joined and returned as a valid record.

        // Write records that span two blocks
        let mut log_test = LogTest::new();
        log_test.write(&big_string("foo".as_bytes(), BLOCK_SIZE));
        log_test.write(&big_string("bar".as_bytes(), BLOCK_SIZE));
        log_test.write("correct".as_bytes());

        // Wipe the middle block
        for offset in BLOCK_SIZE..2 * BLOCK_SIZE {
            log_test.set_byte(offset, b'x');
        }
        assert_eq!("correct".as_bytes(), log_test.read());
        assert_eq!("EOF".as_bytes(), log_test.read());
        let dropped = log_test.dropped_bytes();
        assert!(dropped >= 2 * BLOCK_SIZE);
        assert!(dropped <= 2 * BLOCK_SIZE + 100);
    }

    #[test]
    fn test_log_read_start() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(0, 0);
    }

    #[test]
    fn test_log_read_second_one_off() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(1, 1);
    }

    #[test]
    fn test_log_read_second_ten_thousand() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(10000, 1);
    }

    #[test]
    fn test_log_read_second_start() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(10007, 1);
    }

    #[test]
    fn test_log_read_third_one_off() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(10008, 2);
    }

    #[test]
    fn test_log_read_third_start() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(20014, 2);
    }

    #[test]
    fn test_log_read_fourth_one_off() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(20015, 3);
    }

    #[test]
    fn test_log_read_fourth_first_block_trailer() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(BLOCK_SIZE - 4, 3);
    }

    #[test]
    fn test_log_read_fourth_middle_block() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(BLOCK_SIZE + 1, 3);
    }

    #[test]
    fn test_log_read_fourth_last_block() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(2 * BLOCK_SIZE + 1, 3);
    }

    #[test]
    fn test_log_read_fourth_start() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(
            2 * (HEADER_SIZE + 1000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE,
            3,
        );
    }

    #[test]
    fn test_log_read_initial_offset_into_block_padding() {
        let mut log_test = LogTest::new();
        log_test.check_initial_offset_record(3 * BLOCK_SIZE - 3, 5);
    }

    #[test]
    fn test_log_read_end() {
        let mut log_test = LogTest::new();
        log_test.check_offset_past_end_returns_no_records(0);
    }

    #[test]
    fn test_log_read_past_end() {
        let mut log_test = LogTest::new();
        log_test.check_offset_past_end_returns_no_records(5);
    }
}
