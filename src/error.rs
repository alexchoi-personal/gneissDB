use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Corruption detected: {0}")]
    Corruption(String),

    #[error("Invalid CRC: expected {expected}, got {actual}")]
    InvalidCrc { expected: u32, actual: u32 },

    #[error("Invalid magic number")]
    InvalidMagic,

    #[error("Key not found")]
    NotFound,

    #[error("Database already exists at path")]
    AlreadyExists,

    #[error("Database not found at path")]
    DatabaseNotFound,

    #[error("WAL recovery failed: {0}")]
    WalRecovery(String),

    #[error("Compaction failed: {0}")]
    Compaction(String),

    #[error("Database is shutting down")]
    ShuttingDown,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Write stalled: L0 file count exceeds limit")]
    WriteStall,
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Corruption("test".to_string());
        assert_eq!(err.to_string(), "Corruption detected: test");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_invalid_crc_display() {
        let err = Error::InvalidCrc {
            expected: 123,
            actual: 456,
        };
        assert!(err.to_string().contains("123"));
        assert!(err.to_string().contains("456"));
    }

    #[test]
    fn test_all_error_variants() {
        let errors: Vec<Error> = vec![
            Error::Io(std::io::Error::new(std::io::ErrorKind::Other, "test")),
            Error::Corruption("test".into()),
            Error::InvalidCrc {
                expected: 0,
                actual: 1,
            },
            Error::InvalidMagic,
            Error::NotFound,
            Error::AlreadyExists,
            Error::DatabaseNotFound,
            Error::WalRecovery("test".into()),
            Error::Compaction("test".into()),
            Error::ShuttingDown,
            Error::InvalidArgument("test".into()),
            Error::WriteStall,
        ];

        for err in errors {
            let _ = err.to_string();
        }
    }
}
