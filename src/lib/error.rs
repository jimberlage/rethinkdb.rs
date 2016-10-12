use std::fmt::{self, Display, Formatter};

pub enum Error {
    ReqlAuthError,
    ServerError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            &Error::ReqlAuthError => write!(f, "Authentication failed."),
            &Error::ServerError(ref error) => write!(f, "{}", error),
        }
    }
}
