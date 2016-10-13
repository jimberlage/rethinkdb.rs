use protobuf::ProtobufEnum;
use rustc_serialize::json::{Json, ToJson};
use super::super::ql2::Term_TermType;

/// Each ReQL query/command is represented as a tree, which is serialized to a JSON array like:
///
/// ```
/// [<term>, <tree0>, <tree1>, ...]
/// ```
///
/// An array is a new query, unless it is prefixed with the MAKE_ARRAY term, in which case what
/// follows is a plain JSON array.  Plain JSON elements are represented with the Tree::Datum type.
pub enum Tree {
    Query {
        head: Term_TermType,
        tail: Vec<Tree>,
    },
    Datum(Json)
}

impl ToJson for Tree {
    fn to_json(&self) -> Json {
        match self {
            &Tree::Query {
                head,
                ref tail,
            } => {
                // Return a JSON array, with the term first (as JSON).  Then, recursively convert
                // any other parts of the query to JSON.
                let mut array = vec![head.value().to_json()];
                array.extend(tail.iter().map(|tree| tree.to_json()).collect::<Vec<Json>>());

                Json::Array(array)
            },
            // We need to transform JSON arrays to the special RethinkDB representation, which uses
            // the MAKE_ARRAY term.
            &Tree::Datum(Json::Array(ref array)) => Json::Array(vec![
                Term_TermType::MAKE_ARRAY.value().to_json(),
                Json::Array(array.iter().cloned().collect::<Vec<Json>>()),
            ]),
            &Tree::Datum(ref json) => json.clone(),
        }
    }
}
