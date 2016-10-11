use connection::Connection;

///
/// let mut rethinkdb = RethinkDB::connect("localhost", 7888, "AUTH", 3);
/// db("test").table_create("person_create").replicas(1i32).run(&mut rethinkdb);
/// ```

pub struct RethinkDB {
    pool: Connection
}
