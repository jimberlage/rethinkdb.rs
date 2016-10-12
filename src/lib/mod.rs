#[warn(unused_imports)]
extern crate byteorder;
extern crate protobuf;
extern crate rustc_serialize;
extern crate scram;

#[macro_use]
mod macros;

mod connection;
mod db;
mod error;
mod query;
mod ql2;
mod test;

use db::DB;
use error::Error;

pub fn db() -> Result<DB, Error> {
    Ok(DB{})
}
