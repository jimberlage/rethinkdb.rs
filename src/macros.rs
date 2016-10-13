/// Like the original try macro, but it attempts to coerce the argument to our own Error type.
/// This is indispensible given the number of calls to try! below.
#[macro_export]
macro_rules! my_try {
    ($e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(error) => return Err($crate::error::Error::ServerError(format!("{}", error))),
        }
    }}
}
