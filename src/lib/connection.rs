use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use protobuf::core::parse_from_bytes;
use protobuf::error::ProtobufError;
use protobuf::stream::CodedOutputStream;
use ql2::*;
use std::fmt::Display;
use std::io::{self, BufReader, Write, Read, BufRead};
use std::marker::Sized;
use std::net::TcpStream;
use std::u32;

/// Represents a database connection.
pub struct Connection {
    pub host: String,
    pub port: u16,
    stream:   TcpStream,
    auth:     Option<String>,
}

pub enum UserError {
    AuthorizationKeyTooLarge(usize),
}

pub enum Error {
    UserError(UserError),
    ServerError(Display),
}

impl<T: Display + Sized> From<T> for Error {
    fn from(err: T) -> Error {
        Error::ServerError(err)
    }
}

/// Like the original try macro, but it attempts to coerce the argument to our own Error type.
/// This is indispensible given the number of calls to try! below.
macro_rules! try {
    ($e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(error) => return Err(error as $crate::connection::Error),
        }
    }}
}

impl Connection {
    /// Writes a magic number (32-bit little-endian) to the RethinkDB server, as described in
    /// [ql2.proto](https://github.com/rethinkdb/rethinkdb/blob/next/src/rdb_protocol/ql2.proto).
    fn write_magic_number(&mut self, n: u32) -> Result<(), Error> {
        try!(self.stream.write_u32::<LittleEndian>(n));

        Ok(())
    }

    fn write_authorization_key(&self) -> Result<(), Error> {
        match self.auth {
            Some(ref key) => {
                let key_bytes = key.as_bytes();
                let n = key_bytes.len();

                if n > (u32::MAX as usize) {
                    Err(Error::UserError(UserError::AuthorizationKeyTooLarge(n)))
                } else {
                    try!(self.write_magic_number(n as u32));

                    try!(self.stream.write(&key_bytes));

                    Ok(())
                }
            },
            None => self.write_magic_number(0),
        }
    }

    pub fn handshake(&mut self) -> Result<(), Error> {
        // Send the magic number for V0_4.
        try!(self.write_magic_number(VersionDummy_Version::V0_4 as u32));

        // Send the authorization key, or nothing.
        try!(self.write_authorization_key());

        // Send the magic number for the protocol.
        try!(self.write_magic_number(VersionDummy_Protocol::PROTOBUF as u32));

        try!(self.stream.flush());

        let mut recv = vec![];

        // Read until NULL.
        match BufReader::new(&self.stream).read_until(0, &mut recv) {
            Ok(_) => {
                let _ = recv.pop();
                match String::from_utf8(recv) {
                    Ok(s) => if s.as_str() == "SUCCESS" {
                        Ok(())
                    } else {
                        Err(Error::ServerError(s))
                    },
                    Err(error) => Err(Error::ServerError(error)),
                }
            },
            Err(error) => Err(Error::ServerError(error)),
        }
    }

    /// Connects to the provided server `host` and `port`. `auth` is used for authentication.
    pub fn connect(host: &str, port: u16, auth: &str) -> Result<Connection, Error> {
        let stream = try!(TcpStream::connect((host, port)));
        let mut conn = Connection{
            host:   host.to_string(),
            port:   port,
            stream: stream,
            auth:   Some(auth.to_string())
        };

        try!(conn.handshake());

        Ok(conn)
    }

    fn query(&mut self, query: &Query) -> Result<Response, Error> {
        // Write the size of the incoming protobuf.
        try!(self.write_magic_number(query.compute_size()));

        // Write the actual protobuf.
        let writer = CodedOutputStream::new(&self.stream);

        try!(writer.write_message_no_tag::<Query>(query));

        // Clear the stream.  This or the below call to .flush() may be redundant, but I don't think
        // having both in there hurts.
        try!(writer.flush());

        try!(self.stream.flush());

        // Create a buffered reader to avoid making lots of TCP calls.
        let mut buffered_reader = BufReader::new(&self.stream);

        // Read the size of the new protobuf.
        let len = try!(buffered_reader.read_i32());

        // Now, take only that many bytes off the stream.
        let mut recv = vec![];

        try!(buffered_reader.take(len as u64).read(&mut recv));

        // And, attempt to read the response protobuf.
        let resp = try!(parse_from_bytes::<Response>(&recv));

        // Return the response.
        Ok(resp)
    }
}
