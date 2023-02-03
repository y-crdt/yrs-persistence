use lmdb_rs::MdbError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    DbError(#[from] MdbError),
    #[error("decoding error: {0}")]
    DecodingError(#[from] lib0::error::Error),
    #[error("couldn't parse key entry")]
    KeyError(Vec<u8>),
}
