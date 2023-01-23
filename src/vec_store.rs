use async_trait::async_trait;
use bytes::Bytes;
use std::ops::Range;

use crate::{
    async_store::AsyncStore,
    sync_store::{StoreCommon, SyncStore},
    tree::*,
};

/// A simple in-memory store
///
/// Can be used both synchronously and asynchronously
pub struct VecStore {
    block_level: BlockLevel,
    tree: Vec<blake3::Hash>,
    tree_bitmap: Vec<bool>,
    data: Vec<u8>,
    data_bitmap: Vec<bool>,
}

impl VecStore {
    fn leaf_byte_range_usize(&self, index: BlockNum) -> Range<usize> {
        let range = self.leaf_byte_range(index);
        range.start.to_usize()..range.end.to_usize()
    }
}

impl StoreCommon for VecStore {
    fn tree_len(&self) -> NodeNum {
        NodeNum(self.tree.len() as u64)
    }

    fn data_len(&self) -> ByteNum {
        ByteNum(self.data.len() as u64)
    }

    fn block_level(&self) -> BlockLevel {
        self.block_level
    }
}

impl SyncStore for VecStore {
    type IoError = std::convert::Infallible;
    fn get_hash(&self, offset: NodeNum) -> Result<Option<blake3::Hash>, Self::IoError> {
        let offset = offset.to_usize();
        if offset >= self.tree.len() {
            panic!()
        }
        Ok(if self.tree_bitmap[offset] {
            Some(self.tree[offset])
        } else {
            None
        })
    }
    fn set_hash(
        &mut self,
        offset: NodeNum,
        hash: Option<blake3::Hash>,
    ) -> Result<(), Self::IoError> {
        let offset = offset.to_usize();
        if offset >= self.tree.len() {
            panic!()
        }
        if let Some(hash) = hash {
            self.tree[offset] = hash;
            self.tree_bitmap[offset] = true;
        } else {
            self.tree[offset] = zero_hash();
            self.tree_bitmap[offset] = false;
        }
        Ok(())
    }
    fn get_block(&self, block: BlockNum) -> Result<Option<&[u8]>, Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        Ok(if self.data_bitmap[offset] {
            let range = self.leaf_byte_range_usize(block);
            Some(&self.data[range])
        } else {
            None
        })
    }
    fn set_block(&mut self, block: BlockNum, data: Option<&[u8]>) -> Result<(), Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        let range = self.leaf_byte_range_usize(block);
        if let Some(data) = data {
            self.data_bitmap[offset] = true;
            self.data[range].copy_from_slice(data);
        } else {
            self.data_bitmap[offset] = false;
            self.data[range].fill(0);
        }
        Ok(())
    }
    fn empty(block_level: BlockLevel) -> Result<Self, Self::IoError> {
        let tree_len_usize = 1;
        Ok(VecStore {
            tree: vec![empty_root_hash(); tree_len_usize],
            tree_bitmap: vec![true; tree_len_usize],
            block_level,
            data: Vec::new(),
            data_bitmap: Vec::new(),
        })
    }
    fn grow_storage(&mut self, new_len: ByteNum) -> Result<(), Self::IoError> {
        if new_len < self.data_len() {
            panic!();
        }
        if new_len == self.data_len() {
            return Ok(());
        }
        let blocks = blocks(new_len, self.block_level());
        self.data.resize(new_len.to_usize(), 0u8);
        self.data_bitmap.resize(blocks.to_usize(), false);
        let new_tree_len = num_hashes(blocks);
        self.tree.resize(new_tree_len.to_usize(), zero_hash());
        self.tree_bitmap.resize(new_tree_len.to_usize(), false);
        Ok(())
    }
}

#[async_trait]
impl AsyncStore for VecStore {
    type IoError = std::convert::Infallible;
    async fn get_hash(&self, offset: NodeNum) -> Result<Option<blake3::Hash>, Self::IoError> {
        let offset = offset.to_usize();
        if offset >= self.tree.len() {
            panic!()
        }
        Ok(if self.tree_bitmap[offset] {
            Some(self.tree[offset])
        } else {
            None
        })
    }
    async fn set_hash(
        &mut self,
        offset: NodeNum,
        hash: Option<blake3::Hash>,
    ) -> Result<(), Self::IoError> {
        let offset = offset.to_usize();
        if offset >= self.tree.len() {
            panic!()
        }
        if let Some(hash) = hash {
            self.tree[offset] = hash;
            self.tree_bitmap[offset] = true;
        } else {
            self.tree[offset] = zero_hash();
            self.tree_bitmap[offset] = false;
        }
        Ok(())
    }
    async fn get_block(&self, block: BlockNum) -> Result<Option<Bytes>, Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        Ok(if self.data_bitmap[offset] {
            let range = self.leaf_byte_range_usize(block);
            Some(self.data[range].to_vec().into())
        } else {
            None
        })
    }
    async fn set_block(
        &mut self,
        block: BlockNum,
        data: Option<Bytes>,
    ) -> Result<(), Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        let range = self.leaf_byte_range_usize(block);
        if let Some(data) = data {
            self.data_bitmap[offset] = true;
            self.data[range].copy_from_slice(&data);
        } else {
            self.data_bitmap[offset] = false;
            self.data[range].fill(0);
        }
        Ok(())
    }
    async fn empty(block_level: BlockLevel) -> Self {
        let tree_len_usize = 1;
        Self {
            tree: vec![empty_root_hash(); tree_len_usize],
            tree_bitmap: vec![true; tree_len_usize],
            block_level,
            data: Vec::new(),
            data_bitmap: Vec::new(),
        }
    }
    async fn grow_storage(&mut self, new_len: ByteNum) -> Result<(), Self::IoError> {
        if new_len < self.data_len() {
            panic!();
        }
        if new_len == self.data_len() {
            return Ok(());
        }
        let blocks = blocks(new_len, self.block_level());
        self.data.resize(new_len.to_usize(), 0u8);
        self.data_bitmap.resize(blocks.to_usize(), false);
        let new_tree_len = num_hashes(blocks);
        self.tree.resize(new_tree_len.to_usize(), zero_hash());
        self.tree_bitmap.resize(new_tree_len.to_usize(), false);
        Ok(())
    }
}
