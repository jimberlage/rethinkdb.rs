use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use protobuf::Message;
use protobuf::core::parse_from_bytes;
use protobuf::stream::CodedOutputStream;
use ql2::*;
use rustc_serialize::json::{self, Json};
use scram::{ClientFinal, ClientFirst, ServerFinal, ServerFirst};
// NOTE: Think of this like an Atom in Clojure.  It allows local mutability.
use std::cell::{Ref, RefCell, RefMut};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::io::{BufReader, Write, Read, BufRead};
use std::net::TcpStream;
use std::u32;

const SUB_PROTOCOL_VERSION: i64 = 0;

/// Represents a database connection.
pub struct Connection {
    pub host:     String,
    pub port:     u16,
    pub user:     String,
    pub password: String,
    stream:       RefCell<TcpStream>,
}

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

/// Like the original try macro, but it attempts to coerce the argument to our own Error type.
/// This is indispensible given the number of calls to try! below.
macro_rules! my_try {
    ($e:expr) => {{
        match $e {
            Ok(x) => x,
            Err(error) => return Err($crate::connection::Error::ServerError(format!("{}", error))),
        }
    }}
}

/// The response returned by V1_0 of the RethinkDB handshake protocol, after a protocol version has
/// been successfully set.
#[derive(RustcDecodable)]
struct ProtocolSuccessResponse {
    success: bool,
    min_protocol_version: i64,
    max_protocol_version: i64,
    server_version: String,
}

#[derive(RustcDecodable)]
struct ServerSuccessResponse {
    authentication: String,
    success: bool,
}

#[derive(RustcDecodable)]
struct ServerErrorResponse {
    error: String,
    error_code: i64,
    success: bool,
}

impl Connection {
    fn send_version_number(&self) -> Result<(), Error> {
        my_try!(self.stream.borrow_mut().write_u32::<LittleEndian>(VersionDummy_Version::V1_0 as u32));
        my_try!(self.stream.borrow_mut().flush());

        Ok(())
    }

    fn read_stream_until_null(stream: &TcpStream) -> Result<String, Error> {
        let mut recv = vec![];

        match BufReader::new(stream).read_until(0, &mut recv) {
            Ok(_) => {
                let _ = recv.pop();
                let resp = my_try!(String::from_utf8(recv));

                Ok(resp)
            },
            Err(error) => Err(Error::ServerError(format!("{}", error))),
        }
    }

    /// Reads n bytes off the TCP stream, until a NULL byte is found.  The NULL byte is then
    /// discarded, and the rest of the data is returned as a string.
    fn read_until_null(&self) -> Result<String, Error> {
        let mut result = None;

        Ref::map(self.stream.borrow(), |stream| {
            result = Some(Connection::read_stream_until_null(stream));

            stream
        });

        result.unwrap()
    }

    fn parse_protocol_response(&self) -> Result<ProtocolSuccessResponse, Error> {
        let resp = my_try!(self.read_until_null());

        match json::decode::<ProtocolSuccessResponse>(resp.as_str()) {
            Ok(obj) => if obj.success {
                Ok(obj)
            } else {
                // Should never happen, but better to have the check than not.
                Err(Error::ServerError("Received a success response from RethinkDB with success = false.".to_owned()))
            },
            Err(_) => Err(Error::ServerError(resp)),
        }
    }

    /// Sends the first client handshake response with authentication.  Should send something like:
    ///
    /// ```json
    /// {
    ///   "authentication": "n,,n=user,r=rOprNGfwEbeRWgbNEkqO",
    ///   "authentication_method": "SCRAM-SHA-256",
    ///   "protocol_version": 0
    /// }
    /// ```
    fn send_client_first_message(&self) -> Result<ServerFirst, Error> {
        let client_first = my_try!(ClientFirst::new(&self.user, &self.password, None));
        let (server_first, auth) = client_first.client_first();
        let mut message = BTreeMap::new();
        message.insert("authentication".to_owned(), Json::String(auth));
        let method = "SCRAM-SHA-256".to_owned();
        message.insert("authentication_method".to_owned(), Json::String(method));
        message.insert("protocol_version".to_owned(), Json::I64(SUB_PROTOCOL_VERSION));
        let encoded = my_try!(json::encode(&message));
        my_try!(self.stream.borrow_mut().write(&encoded.as_bytes()));
        my_try!(self.stream.borrow_mut().flush());

        Ok(server_first)
    }

    /// Parses messages from the server, as defined for the RethinkDB handshake in
    /// https://rethinkdb.com/docs/writing-drivers/
    fn parse_server_message(&self) -> Result<ServerSuccessResponse, Error> {
        let resp = my_try!(self.read_until_null());

        match json::decode::<ServerSuccessResponse>(resp.as_str()) {
            Ok(success_obj) => if success_obj.success {
                Ok(success_obj)
            } else {
                // Should never happen, but better to have the check than not.
                Err(Error::ServerError("Received a success response from RethinkDB with success = false.".to_owned()))
            },
            Err(_) => match json::decode::<ServerErrorResponse>(resp.as_str()) {
                Ok(error_obj) => if !error_obj.success {
                    // An error code within [10, 20] is defined to return a ReqlAuthError.
                    if error_obj.error_code >= 10 && error_obj.error_code <= 20 {
                        Err(Error::ReqlAuthError)
                    } else {
                        Err(Error::ServerError(error_obj.error))
                    }
                } else {
                    // Should never happen, but better to have the check than not.
                    Err(Error::ServerError("Received an error response from RethinkDB with success = true.".to_owned()))
                },
                // We don't have either a success or an error response.  Very weird.
                Err(error) => Err(Error::ServerError(format!("{}", error))),
            }
        }
    }

    /// Sends the final client message in the authentication handshake.  Should look like:
    ///
    /// ```json
    /// {
    ///   "authentication": "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    /// }
    /// ```
    fn send_client_final_message(&self, client_final: ClientFinal) -> Result<ServerFinal, Error> {
        let (server_final, auth) = client_final.client_final();
        let mut message = BTreeMap::new();
        message.insert("authentication".to_owned(), auth);
        let encoded = my_try!(json::encode(&message));

        my_try!(self.stream.borrow_mut().write(&encoded.as_bytes()));
        my_try!(self.stream.borrow_mut().flush());

        Ok(server_final)
    }

    /// Uses the handshake for V1_0, defined in https://rethinkdb.com/docs/writing-drivers/.
    pub fn handshake(&self) -> Result<(), Error> {
        my_try!(self.send_version_number());
        let _ = my_try!(self.parse_protocol_response());
        let server_first = my_try!(self.send_client_first_message());
        let client_first_success = my_try!(self.parse_server_message());
        let client_final = my_try!(server_first.handle_server_first(&client_first_success.authentication));
        let server_final = my_try!(self.send_client_final_message(client_final));
        let client_final_success = my_try!(self.parse_server_message());
        my_try!(server_final.handle_server_final(&client_final_success.authentication));

        Ok(())
    }

    /// Connects to the provided server `host` and `port`. `auth` is used for authentication.
    pub fn connect(host: &str, port: u16, user: &str, password: &str) -> Result<Connection, Error> {
        let stream = my_try!(TcpStream::connect((host, port)));
        let mut conn = Connection{
            host:     host.to_string(),
            port:     port,
            stream:   RefCell::new(stream),
            user:     user.to_owned(),
            password: password.to_owned(),
        };

        my_try!(conn.handshake());

        Ok(conn)
    }
}
