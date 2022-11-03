use std::{io, result};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("data store disconnected")]
    AlreadyExist,
    #[error("the data for key is not available")]
    Corruption(String),

    #[error("invalid argument")]
    InvalidArgument(String),

    #[error("invalid data")]
    InvalidData(String),
    #[error("io read error")]
    IOError {
        #[from]
        source: io::Error,
        // backtrace: Backtrace,
    },
}

pub type Result<T> = result::Result<T, Error>;

// impl From<io::Error> for Error {
//     fn from(e: io::Error) -> Self {
//         Error::IOError(e)
//     }
// }
