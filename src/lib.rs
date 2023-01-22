pub mod async_store;
pub mod errors;
pub mod sync_store;
mod tree;
mod vec_store;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compare;

pub struct BlakeFile<S>(S);
pub struct AsyncBlakeFile<S>(S);