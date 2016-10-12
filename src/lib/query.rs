use ql2::{Query, Query_QueryType, Term};
use protobuf::ProtobufEnum;
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

        obj.insert("type".to_owned(), self.get_field_type().to_json());

        Json::Object(obj)
    }
}
