// mod reader;
mod writer;

#[derive(Clone, Copy)]
#[repr(u8)]
enum RecordType {
    Zero = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl From<u8> for RecordType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Zero,
            1 => Self::Full,
            2 => Self::First,
            3 => Self::Middle,
            4 => Self::Last,
            _ => panic!(),
        }
    }
}

const MAX_RECORD_TYPE: RecordType = RecordType::Last;

const BLOCK_SIZE: usize = 32768;

/// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
const HEADER_SIZE: usize = 4 + 2 + 1;
