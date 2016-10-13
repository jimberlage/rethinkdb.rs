use rustc_serialize::json::Json;
use error::Error;

pub struct DB {
}

impl DB {
    fn create(name: &str) -> Result<Json, Error> {
        // TODO: CHANGE ME
        Ok(Json::Null)
    }
}
