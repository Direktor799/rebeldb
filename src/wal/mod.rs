mod writer;

#[derive(Clone, Copy)]
enum RecordType {
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

const MAX_RECORD_TYPE: RecordType = RecordType::Last;

const BLOCK_SIZE: usize = 32768;

/// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const HEADER_SIZE: usize = 4 + 2 + 1;
