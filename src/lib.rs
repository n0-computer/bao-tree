mod async_store;
mod sync_store;
mod tree;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compare;

pub struct BlakeFile<S>(S);
