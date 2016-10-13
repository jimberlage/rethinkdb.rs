use rustc_serialize::json::{Json, ToJson};
use super::ql2::Term_TermType;

// Each ReQL query/command is represented as a tree.
pub enum Tree {
    Query {
        head: Term_TermType,
        tail: Vec<Tree>,
    },
    Datum(Json)
}

// Each tree can be represented in JSON by an array.
impl ToJson for Tree {
    fn to_json(&self) -> Json {
        match self {
            &Tree::Query {
                head: head,
                tail: tail,
            } => {
                let array = vec![head.to_json()];
                array.extend(tail.iter().map(|tree| tree.to_json()).collect::<Vec<Json>>());

                Json::Array(array)
            },
            // We need to transform JSON arrays to the special RethinkDB representation, which uses
            // the MAKE_ARRAY term.
            &Tree::Datum(Json::Array(array)) => {
                Json::Array(vec![Term_TermType::MAKE_ARRAY.to_json(), Json::Array(array.iter().cloned())])
            },
            &Tree::Datum(json) => json.clone(),
        }
    }
}
