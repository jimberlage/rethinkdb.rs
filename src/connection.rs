use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use error::Error;
use ql2::{Term_TermType, VersionDummy_Version};
use reql::tree::Tree;
use rustc_serialize::json::{self, Json, ToJson};
use scram::{ClientFinal, ClientFirst, ServerFinal, ServerFirst};
// NOTE: Think of this like an Atom in Clojure.  It allows local mutability.
use std::cell::{Cell, Ref, RefCell};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::str;
use std::u32;

const SUB_PROTOCOL_VERSION: i64 = 0;

/// Represents a database connection.
pub struct Connection {
    pub host:     String,
    pub port:     u16,
    pub user:     String,
    pub password: String,
    stream:       RefCell<TcpStream>,
    query_token:  Cell<u64>,
}

pub struct QueryResponse {
    query_token: u64,
    length:      u32,
    response:    Json,
}

/// The response returned by V1_0 of the RethinkDB handshake protocol, after a protocol version has
/// been successfully set.
#[derive(Debug,RustcDecodable)]
struct ProtocolSuccessResponse {
    success:              bool,
    min_protocol_version: i64,
    max_protocol_version: i64,
    server_version:       String,
}

#[derive(Debug,RustcDecodable)]
struct ServerSuccessResponse {
    authentication: String,
    success:        bool,
}

#[derive(RustcDecodable)]
struct ServerErrorResponse {
    error:      String,
    error_code: i64,
    success:    bool,
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
        my_try!(self.stream.borrow_mut().write_u8(0));
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
        my_try!(self.stream.borrow_mut().write_u8(0));
        my_try!(self.stream.borrow_mut().flush());

        Ok(server_final)
    }

    /// Uses the handshake for V1_0, defined in https://rethinkdb.com/docs/writing-drivers/.
    fn handshake(&self) -> Result<(), Error> {
        my_try!(self.send_version_number());
        let _ = my_try!(self.parse_protocol_response());
        let server_first = my_try!(self.send_client_first_message());
        let client_first_response = my_try!(self.parse_server_message());
        let client_final = my_try!(server_first.handle_server_first(&client_first_response.authentication));
        let server_final = my_try!(self.send_client_final_message(client_final));
        let client_final_response = my_try!(self.parse_server_message());
        my_try!(server_final.handle_server_final(&client_final_response.authentication));

        Ok(())
    }

    /// Connects to the provided server `host` and `port`.
    pub fn connect(host: &str, port: u16, user: &str, password: &str) -> Result<Connection, Error> {
        let stream = my_try!(TcpStream::connect((host, port)));
        let conn = Connection{
            host:        host.to_string(),
            port:        port,
            stream:      RefCell::new(stream),
            query_token: Cell::new(0),
            user:        user.to_owned(),
            password:    password.to_owned(),
        };

        match conn.handshake() {
            Ok(()) => Ok(conn),
            Err(error) => {
                Ref::map(conn.stream.borrow(), |stream| {
                    stream.shutdown(Shutdown::Both).unwrap();

                    stream
                });

                Err(error)
            },
        }
    }

    fn send_query(&self, tree: &Tree) -> Result<(), Error> {
        let token = self.query_token.get();
        // Increment the token for the next request.
        self.query_token.set(token.wrapping_add(1));
        let tree = my_try!(json::encode(&tree.to_json()));
        let len = tree.as_bytes().len();
        if len > (u32::MAX as usize) {
            return Err(Error::QueryTooLarge(len));
        }

        my_try!(self.stream.borrow_mut().write_u64::<LittleEndian>(token));
        my_try!(self.stream.borrow_mut().write_u32::<LittleEndian>(len as u32));
        my_try!(self.stream.borrow_mut().write(&tree.as_bytes()));

        Ok(())
    }

    // TODO: Worry about how to handle unordered responses.  We should probably loop over the
    // stream once the handshake is done, trying to read each response and putting it in a hashed
    // collection.
    fn parse_response_from_stream(stream: &TcpStream) -> Result<QueryResponse, Error> {
        let mut reader = BufReader::new(stream);
        let token = my_try!(reader.read_u64::<LittleEndian>());
        let len = my_try!(reader.read_u32::<LittleEndian>());
        let mut recv = vec![];
        let _ = my_try!(reader.take(len as u64).read(&mut recv));
        let response = my_try!(Json::from_str(my_try!(str::from_utf8(&recv))));

        Ok(QueryResponse {
            query_token: token,
            length:      len,
            response:    response,
        })
    }

    fn parse_response(&self) -> Result<QueryResponse, Error> {
        let mut result = None;

        Ref::map(self.stream.borrow(), |stream| {
            result = Some(Connection::parse_response_from_stream(stream));

            stream
        });

        result.unwrap()
    }

    pub fn db_create(&self, name: &str) -> Result<QueryResponse, Error> {
        my_try!(self.send_query(&Tree::Query {
            head: Term_TermType::DB_CREATE,
            tail: vec![Tree::Datum(Json::String(name.to_owned()))],
        }));
        Ok(my_try!(self.parse_response()))
    }
}
