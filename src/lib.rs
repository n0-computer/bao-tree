pub mod async_store;
pub mod errors;
pub mod sync_store;
#[macro_use]
mod macros;
mod tree;
mod vec_store;

mod bao_tree;
mod range_spec;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compare;

pub struct BlakeFile<S>(S);
pub struct AsyncBlakeFile<S>(S);
