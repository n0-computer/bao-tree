//! Tests
//!
//! - [io]: every encoder, decoder, outboard and validator, sync, fsm and
//!   mixed, keyed and unkeyed, must agree with each other and with the
//!   expected results.
//! - [tree]: tree geometry and traversals.
//! - [bao]: compatibility with the bao crate.
//! - [hash]: keyed hash primitives.
mod bao;
mod hash;
mod io;
mod tree;
