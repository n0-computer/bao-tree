//! Tests
//!
//! - [io]: every encoder, decoder, outboard and validator, sync, fsm and
//!   mixed, keyed and unkeyed, must agree with each other and with the
//!   expected results.
//! - [tree]: tree geometry and traversals.
//! - [bao]: compatibility with the bao crate.
//! - [hash]: keyed hash primitives.
//! - [reference]: reference implementations and helpers shared by the tests.
//! - [coverage]: calls trivial code, so the coverage report shows what is
//!   actually untested.
mod bao;
mod coverage;
mod hash;
mod io;
mod reference;
mod tree;
