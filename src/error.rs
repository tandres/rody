
use thiserror::Error;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Bad magic in source file")]
    BadMagic,
    #[error("Invalid header version")]
    InvalidVersion,
    #[error("IoError")]
    IoError(#[from] io::Error),
    #[error("Internal")]
    Internal(String),
    #[error("Block Too Large")]
    TooLarge(usize),
}

// Can't use AsRef<str> here because io::Error does too
impl From<String> for Error {
    fn from(src: String) -> Self {
        Error::Internal(src)
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Self {
        Error::from(src.to_string())
    }
}
