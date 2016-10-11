use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use protobuf::Message;
use protobuf::core::parse_from_bytes;
use protobuf::stream::CodedOutputStream;
use ql2::*;
use std::fmt::{self, Display, Formatter};
use std::io::{BufReader, Write, Read, BufRead};
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
    ServerError(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            &Error::UserError(UserError::AuthorizationKeyTooLarge(n)) => {
                write!(f, "Authorization key cannot exceed {} bytes - found {}", u32::MAX, n)
            },
            &Error::ServerError(ref s) => write!(f, "{}", s),
        }
    }
}

/// Like the original try macro, but it attempts to coerce the argument to our own Error type.
/// This is indispensible given the number of calls to try! below.
macro_rules! try {
    ($e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(error) => return Err($crate::connection::Error::ServerError(format!("{}", error))),
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

    /// Writes the authorization key (if any) to the RethinkDB server.  If none exists, it will
    /// still write a length of 0 bytes.
    fn write_authorization_key(&mut self) -> Result<(), Error> {
        match self.auth.clone() {
            Some(ref key) => {
                let key_bytes = key.as_bytes();
                let n = key_bytes.len();

                if n > (u32::MAX as usize) {
                    Err(Error::UserError(UserError::AuthorizationKeyTooLarge(n)))
                } else {
                    // Write the length of the key.
                    try!(self.write_magic_number(n as u32));

                    // Write the key itself.
                    try!(self.stream.write(&key_bytes));

                    Ok(())
                }
            },
            // Write the length of the (nonexistent) key.
            None => self.write_magic_number(0),
        }
    }

    pub fn handshake(&mut self) -> Result<(), Error> {
        // Send the magic number for V0_4.
        try!(self.write_magic_number(VersionDummy_Version::V0_4 as u32));

        try!(self.write_authorization_key());

        // Send the magic number for the protocol.
        try!(self.write_magic_number(VersionDummy_Protocol::JSON as u32));

        try!(self.stream.flush());

        let mut recv = vec![];

        // Read until NULL.
        match BufReader::new(&self.stream).read_until(0, &mut recv) {
            Ok(_) => {
                let _ = recv.pop();
                match String::from_utf8(recv) {
                    // RethinkDB indicates that the handshake was successful by sending a
                    // NULL-terminated SUCCESS string.
                    Ok(s) => if s.as_str() == "SUCCESS" {
                        Ok(())
                    } else {
                        Err(Error::ServerError(s))
                    },
                    Err(error) => Err(Error::ServerError(format!("{}", error))),
                }
            },
            Err(error) => Err(Error::ServerError(format!("{}", error))),
        }
    }

    /// Connects to the provided server `host` and `port`. `auth` is used for authentication.
    pub fn connect(host: &str, port: u16, auth: Option<&str>) -> Result<Connection, Error> {
        let stream = try!(TcpStream::connect((host, port)));
        let mut conn = Connection{
            host:   host.to_string(),
            port:   port,
            stream: stream,
            auth:   auth.map(|s| s.to_owned()),
        };

        try!(conn.handshake());

        Ok(conn)
    }

    fn write_query(&mut self, query: &Query) -> Result<(), Error> {
        // Write the size of the incoming protobuf.
        try!(self.write_magic_number(query.compute_size()));

        // Write the actual protobuf.
        let mut writer = CodedOutputStream::new(&mut self.stream);

        try!(writer.write_message_no_tag::<Query>(query));

        try!(writer.flush());

        Ok(())
    }

    fn read_query_response(&self) -> Result<Response, Error> {
        // Create a buffered reader to avoid making lots of TCP calls.
        let mut buffered_reader = BufReader::new(&self.stream);

        // Read the size of the new protobuf.
        let len = try!(buffered_reader.read_u32::<LittleEndian>());

        // Now, take only that many bytes off the stream.
        let mut recv = vec![];

        try!(buffered_reader.take(len as u64).read(&mut recv));

        // And, attempt to read the response protobuf.
        let resp = try!(parse_from_bytes::<Response>(&recv));

        // Return the response.
        Ok(resp)
    }

    pub fn query(&mut self, query: &Query) -> Result<Response, Error> {
        try!(self.write_query(query));
        let resp = try!(self.read_query_response());

        Ok(resp)
    }
}
