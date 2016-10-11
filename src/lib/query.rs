use ql2::{Query, Query_QueryType, Term};
use rustc_serialize::json::{Json, ToJson};
use std::collections::BTreeMap;

impl ToJson for Query_QueryType {
    fn to_json(&self) -> Json {
        Json::I64(self.value() as i64)
    }
}

impl ToJson for Query {
    fn to_json(&self) -> Json {
        let mut obj = BTreeMap::new();

        let field_type = match self.field_type {
            Some(field_type) => field_type.to_json(),
            None => Query_QueryType::START.to_json(),
        }
        obj.insert("type".to_owned(), field_type);
    }
}
