/// Like the original try macro, but it attempts to coerce the argument to our own Error type.
///
/// Example:
/// ```rust
/// use error::Error;
///
/// fn always_errors() -> Result<(), String> {
///   "I always error, no matter what".to_owned()
/// }
///
/// fn do_stuff() -> Result<String, Error> {
///   my_try!(always_errors());
///
///   Ok("I didn't do that much, if I'm being honest")
/// }
/// ```
///
/// This will expand to:
///
/// ```rust
/// use error::Error;
///
/// fn always_errors() -> Result<(), String> {
///   "I always error, no matter what".to_owned()
/// }
///
/// fn do_stuff() -> Result<String, Error> {
///   match always_errors() {
///     Ok(x) => x,
///     Err(error) => return Err(Error::ServerError(format!("{}", error))),
///   };
///
///   Ok("I didn't do that much, if I'm being honest")
/// }
/// ```
#[macro_export]
macro_rules! my_try {
    ($e:expr) => {{
        // Match whatever $e evaluates to (a Result of some sort)
        match $e {
            Ok(x) => x,
            // If we got an error, coerce it to our own error type.
            //
            // This requires that `error` implements
            // [std::fmt::Display](https://doc.rust-lang.org/std/fmt/trait.Display.html).
            Err(error) => return Err($crate::error::Error::ServerError(format!("{}", error))),
        }
    }}
}
