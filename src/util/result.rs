// TODO: refactor this later
use std::{error, fmt::Display};

#[derive(Debug, Clone, PartialEq)]
enum Code {
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IOError = 5,
}

impl Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Code::NotFound => "NotFound",
            Code::Corruption => "Corruption",
            Code::NotSupported => "NotSupported",
            Code::InvalidArgument => "InvalidArgument",
            Code::IOError => "IOError",
        };
        f.write_str(str)
    }
}

#[derive(Debug, Clone)]
pub struct Error {
    code: Code,
    msg: String,
}

impl Error {
    pub fn not_found(msg: &str) -> Self {
        Self {
            code: Code::NotFound,
            msg: msg.to_owned(),
        }
    }

    pub fn corruption(msg: &str) -> Self {
        Self {
            code: Code::Corruption,
            msg: msg.to_owned(),
        }
    }

    pub fn not_supported(msg: &str) -> Self {
        Self {
            code: Code::NotSupported,
            msg: msg.to_owned(),
        }
    }

    pub fn invalid_argument(msg: &str) -> Self {
        Self {
            code: Code::InvalidArgument,
            msg: msg.to_owned(),
        }
    }

    pub fn io_error(msg: &str) -> Self {
        Self {
            code: Code::IOError,
            msg: msg.to_owned(),
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.code == Code::NotFound
    }

    pub fn is_corruption(&self) -> bool {
        self.code == Code::Corruption
    }

    pub fn is_not_supported(&self) -> bool {
        self.code == Code::NotSupported
    }

    pub fn is_invalid_argument(&self) -> bool {
        self.code == Code::InvalidArgument
    }

    pub fn is_io_error(&self) -> bool {
        self.code == Code::IOError
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}: {}", self.code.to_string(), self.msg))
    }
}

impl error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::Result;
    use crate::util::result::Error;

    #[test]
    fn test_result_move() {
        let ok: Result<()> = Ok(());
        let ok2 = ok;
        assert!(ok2.is_ok());

        let status: Result<()> = Err(Error::not_found("custom NotFound message"));
        let status2 = status;
        let error = status2.unwrap_err();
        assert!(error.is_not_found());
        assert_eq!("NotFound: custom NotFound message", error.to_string());
    }
}
