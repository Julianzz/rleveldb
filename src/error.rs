use std::{io, result, string::FromUtf8Error};

use crossbeam::channel::RecvError;
use thiserror::Error;

use crate::env;

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
        source: env::IoError,
        // backtrace: Backtrace,
    },
    #[error("format error ")]
    FormatError {
        #[from]
        source: io::Error,
    },

    #[error("error in receive from channel")]
    ReceiveError {
        #[from]
        source: RecvError,
    },
    #[error("not find")]
    NotFoundError(String),

    #[error("custom error")]
    CustomError(String),

    #[error("utf8-error")]
    FromUtf8Error {
        #[from]
        source: FromUtf8Error,
    },
}

pub type Result<T> = result::Result<T, Error>;

// impl From<io::Error> for Error {
//     fn from(e: io::Error) -> Self {
//         Error::IOError(e)
//     }
// }
