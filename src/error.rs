use std::fmt::{self, Display, Formatter};
use std::u32;

pub enum Error {
    QueryTooLarge(usize),
    ReqlAuthError,
    ServerError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            &Error::QueryTooLarge(n) => write!(f, "Query was too large: max size is {} bytes but the query takes up {} bytes.", u32::MAX, n),
            &Error::ReqlAuthError => write!(f, "Authentication failed."),
            &Error::ServerError(ref error) => write!(f, "{}", error),
        }
    }
}
