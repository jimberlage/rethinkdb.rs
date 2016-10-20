# rethinkdb.rs

This is a Rust driver for [RethinkDB](https://rethinkdb.com).  It is *not* in a stable state yet, but it's getting there.  We are targeting V1\_0 of the RethinkDB protocol.  Other versions will probably not be supported anytime soon, though I'm certainly open to having them.

## Useful Links

- [Writing Drivers](https://rethinkdb.com/docs/writing-drivers/) describes the initial handshake and how queries should be serialized
- [ql2.proto](https://github.com/jimberlage/rethinkdb.rs/blob/master/ql2.proto) describes the possible commands for RethinkDB
