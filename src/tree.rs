use std::ops::{Add, Div, Mul, Range, Sub};

macro_rules! index_newtype {
    (
        $(#[$outer:meta])*
        pub struct $name:ident(pub $wrapped:ty);
    ) => {
        $(#[$outer])*
        #[repr(transparent)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub $wrapped);


        impl Mul<$wrapped> for $name {
            type Output = $name;

            fn mul(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 * rhs)
            }
        }

        impl Div<$wrapped> for $name {
            type Output = $name;

            fn div(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 / rhs)
            }
        }

        impl Sub<$wrapped> for $name {
            type Output = $name;

            fn sub(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 - rhs)
            }
        }

        impl Sub<$name> for $name {
            type Output = $name;

            fn sub(self, rhs: $name) -> Self::Output {
                $name(self.0 - rhs.0)
            }
        }

        impl Add<$wrapped> for $name {
            type Output = $name;

            fn add(self, rhs: $wrapped) -> Self::Output {
                $name(self.0 + rhs)
            }
        }

        impl Add<$name> for $name {
            type Output = $name;

            fn add(self, rhs: $name) -> Self::Output {
                $name(self.0 + rhs.0)
            }
        }

        impl PartialEq<$wrapped> for $name {
            fn eq(&self, other: &$wrapped) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for $wrapped {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        impl PartialOrd<$wrapped> for $name {
            fn partial_cmp(&self, other: &$wrapped) -> Option<std::cmp::Ordering> {
                self.0.partial_cmp(other)
            }
        }

        impl $name {

            /// Convert to usize or panic if it doesn't fit.
            pub fn to_usize(self) -> usize {
                usize::try_from(self.0).expect("usize overflow")
            }
        }
    }
}

index_newtype! {
    /// A number of nodes in the tree
    pub struct Nodes(pub u64);
}

index_newtype! {
    /// A number of <=1024 byte blake3 chunks
    pub struct Chunks(pub u64);
}

impl Chunks {
    fn to_bytes(self) -> Bytes {
        Bytes(self.0 * 1024)
    }
}

index_newtype! {
    /// a number of leaf blocks with its own hash
    pub struct Blocks(pub u64);
}

impl Blocks {
    pub fn to_bytes(self, block_level: u32) -> Bytes {
        let block_size = block_size0(block_level);
        Bytes(self.0 * block_size)
    }
}

index_newtype! {
    /// A number of bytes
    pub struct Bytes(pub u64);
}

#[repr(transparent)]
pub struct BlockLevel(pub u32);

pub fn bo_range_to_usize(range: Range<Bytes>) -> Range<usize> {
    range.start.to_usize()..range.end.to_usize()
}

pub fn block_size(block_level: u32) -> Bytes {
    Bytes(block_size0(block_level))
}

fn block_size0(block_level: u32) -> u64 {
    1024 << block_level
}

pub fn leafs(blocks: Nodes) -> Blocks {
    Blocks((blocks.0 + 1) / 2)
}

/// Root offset given a number of leaves.
pub fn root(leafs: Blocks) -> Nodes {
    Nodes(root0(leafs.0))
}

fn root0(leafs: u64) -> u64 {
    leafs.next_power_of_two() - 1
}

/// Level for an offset. 0 is for leaves, 1 is for the first level of branches, etc.
pub fn level(offset: Nodes) -> u32 {
    level0(offset.0)
}

fn level0(offset: u64) -> u32 {
    (!offset).trailing_zeros()
}

/// Span for an offset. 1 is for leaves, 2 is for the first level of branches, etc.
pub fn span(offset: Nodes) -> Nodes {
    Nodes(span0(offset.0))
}

fn span0(offset: u64) -> u64 {
    1 << (!offset).trailing_zeros()
}

pub fn left_child(offset: Nodes) -> Option<Nodes> {
    left_child0(offset.0).map(Nodes)
}

fn left_child0(offset: u64) -> Option<u64> {
    let span = span0(offset);
    if span == 1 {
        None
    } else {
        Some(offset - span / 2)
    }
}

pub fn right_child(offset: Nodes) -> Option<Nodes> {
    right_child0(offset.0).map(Nodes)
}

fn right_child0(offset: u64) -> Option<u64> {
    let span = span0(offset);
    if span == 1 {
        None
    } else {
        Some(offset + span / 2)
    }
}

/// Get a valid right descendant for an offset
pub fn right_descendant(offset: Nodes, len: Nodes) -> Option<Nodes> {
    let mut offset = right_child(offset)?;
    while offset >= len {
        offset = left_child(offset)?;
    }
    Some(offset)
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
pub fn children(offset: Nodes) -> Option<(Nodes, Nodes)> {
    let span = span(offset);
    if span.0 == 1 {
        None
    } else {
        Some((offset - span / 2, offset + span / 2))
    }
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
pub fn descendants(offset: Nodes, len: Nodes) -> Option<(Nodes, Nodes)> {
    let lc = left_child(offset);
    let rc = right_descendant(offset, len);
    if let (Some(l), Some(r)) = (lc, rc) {
        Some((l, r))
    } else {
        None
    }
}

pub fn is_left_sibling(offset: Nodes) -> bool {
    is_left_sibling0(offset.0)
}

fn is_left_sibling0(offset: u64) -> bool {
    let span = span0(offset) * 2;
    (offset & span) == 0
}

pub fn parent(offset: Nodes) -> Nodes {
    Nodes(parent0(offset.0))
}

fn parent0(offset: u64) -> u64 {
    let span = span0(offset);
    // if is_left_sibling(offset) {
    if (offset & (span * 2)) == 0 {
        offset + span
    } else {
        offset - span
    }
}

/// Get the chunk index for an offset
pub fn index(offset: Nodes) -> Blocks {
    Blocks(offset.0 / 2)
}

pub fn range(offset: Nodes) -> Range<Nodes> {
    let r = range0(offset.0);
    Nodes(r.start)..Nodes(r.end)
}

fn range0(offset: u64) -> Range<u64> {
    let span = span0(offset);
    offset + 1 - span..offset + span
}

pub fn sibling(offset: Nodes) -> Nodes {
    Nodes(sibling0(offset.0))
}

fn sibling0(offset: u64) -> u64 {
    if is_left_sibling0(offset) {
        offset + span0(offset) * 2
    } else {
        offset - span0(offset) * 2
    }
}

/// depth first, left to right traversal of a tree of size len
pub fn depth_first_left_to_right(len: Nodes) -> impl Iterator<Item = Nodes> {
    fn descend(offset: Nodes, len: Nodes, res: &mut Vec<Nodes>) {
        if offset < len {
            res.push(offset);
            if let Some((left, right)) = children(offset) {
                descend(left, len, res);
                descend(right, len, res);
            }
        } else if let Some(left_child) = left_child(offset) {
            descend(left_child, len, res)
        }
    }
    // compute number of leafs (this will be 1 even for empty data)
    let leafs = leafs(len);
    // compute root offset
    let root = root(leafs);
    // result vec
    let mut res = Vec::with_capacity(len.to_usize());
    descend(root, len, &mut res);
    res.into_iter()
}

/// breadth first, left to right traversal of a tree of size len
pub fn breadth_first_left_to_right(len: Nodes) -> impl Iterator<Item = Nodes> {
    fn descend(current: Vec<Nodes>, len: Nodes, res: &mut Vec<Nodes>) {
        let mut next = Vec::new();
        for offset in current {
            if offset < len {
                res.push(offset);
                if let Some((left, right)) = children(offset) {
                    next.push(left);
                    next.push(right);
                }
            } else if let Some(left_child) = left_child(offset) {
                next.push(left_child);
            }
        }
        if !next.is_empty() {
            descend(next, len, res);
        }
    }
    // compute number of leafs (this will be 1 even for empty data)
    let leafs = leafs(len);
    // compute root offset
    let root = root(leafs);
    // result vec
    let mut res = Vec::with_capacity(len.to_usize());
    descend(vec![root], len, &mut res);
    res.into_iter()
}

#[cfg(test)]
mod tests {
    use std::cmp::{max, min};

    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for Nodes {
        type Parameters = ();
        type Strategy = BoxedStrategy<Nodes>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            any::<u64>().prop_map(Nodes).boxed()
        }
    }

    proptest! {

        #[test]
        fn children_parent(i in any::<Nodes>()) {
            if let Some((l, r)) = children(i) {
                assert_eq!(parent(l), i);
                assert_eq!(parent(r), i);
            }
        }

        /// Checks that left_child/right_child are consistent with children
        #[test]
        fn children_consistent(i in any::<Nodes>()) {
            let lc = left_child(i);
            let rc = right_child(i);
            let c = children(i);
            let lc1 = c.map(|(l, _)| l);
            let rc1 = c.map(|(_, r)| r);
            assert_eq!(lc, lc1);
            assert_eq!(rc, rc1);
        }

        #[test]
        fn sibling_sibling(i in any::<Nodes>()) {
            let s = sibling(i);
            let distance = max(s, i) - min(s, i);
            // sibling is at a distance of 2*span
            assert_eq!(distance, span(i) * 2);
            // sibling of sibling is value itself
            assert_eq!(sibling(s), i);
        }

        #[test]
        fn compare_descendants(i in any::<Nodes>(), len in any::<Nodes>()) {
            let d = descendants(i, len);
            let lc = left_child(i);
            let rc = right_descendant(i, len);
            if let (Some(lc), Some(rc)) = (lc, rc) {
                assert_eq!(d, Some((lc, rc)));
            } else {
                assert_eq!(d, None);
            }
        }

    }

    #[test]
    fn test_right_descendant() {
        for i in (1..11) {
            println!(
                "valid_right_child({}, 9), {:?}",
                i,
                right_descendant(Nodes(i), Nodes(9))
            );
        }
    }

    #[test]
    fn test_left() {
        for i in 0..20 {
            println!("assert_eq!(left_child({}), {:?})", i, left_child(Nodes(i)));
        }
        for i in 0..20 {
            println!("assert_eq!(is_left({}), {})", i, is_left_sibling(Nodes(i)));
        }
        for i in 0..20 {
            println!("assert_eq!(parent({}), {:?})", i, parent(Nodes(i)));
        }
        for i in 0..20 {
            println!("assert_eq!(sibling({}), {:?})", i, sibling(Nodes(i)));
        }
        assert_eq!(left_child0(3), Some(1));
        assert_eq!(left_child0(1), Some(0));
        assert_eq!(left_child0(0), None);
    }

    #[test]
    fn test_span() {
        for i in 0..10 {
            println!("assert_eq!(span({}), {})", i, span0(i))
        }
    }

    #[test]
    fn test_level() {
        for i in 0..10 {
            println!("assert_eq!(level({}), {})", i, level0(i))
        }
        assert_eq!(level0(0), 0);
        assert_eq!(level0(1), 1);
        assert_eq!(level0(2), 0);
        assert_eq!(level0(3), 2);
    }

    #[test]
    fn test_dflr() {
        fn dflr(len: u64) -> impl Iterator<Item = Nodes> {
            depth_first_left_to_right(Nodes(len))
        }
        assert_eq!(dflr(1).collect::<Vec<_>>(), vec![0]);
        assert_eq!(dflr(3).collect::<Vec<_>>(), vec![1, 0, 2]);
        assert_eq!(dflr(5).collect::<Vec<_>>(), vec![3, 1, 0, 2, 4]);
        assert_eq!(dflr(7).collect::<Vec<_>>(), vec![3, 1, 0, 2, 5, 4, 6]);
        assert_eq!(dflr(9).collect::<Vec<_>>(), vec![7, 3, 1, 0, 2, 5, 4, 6, 8]);
    }

    #[test]
    fn test_bflr() {
        fn bflr(len: u64) -> impl Iterator<Item = Nodes> {
            breadth_first_left_to_right(Nodes(len))
        }
        assert_eq!(bflr(1).collect::<Vec<_>>(), vec![0]);
        assert_eq!(bflr(3).collect::<Vec<_>>(), vec![1, 0, 2]);
        assert_eq!(bflr(5).collect::<Vec<_>>(), vec![3, 1, 0, 2, 4]);
        assert_eq!(bflr(7).collect::<Vec<_>>(), vec![3, 1, 5, 0, 2, 4, 6]);
        assert_eq!(bflr(9).collect::<Vec<_>>(), vec![7, 3, 1, 5, 0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_range() {
        for i in 0..8 {
            println!("{} {:?}", i, range0(i));
        }
    }

    #[test]
    fn test_root() {
        assert_eq!(root0(0), 0);
        assert_eq!(root0(1), 0);
        assert_eq!(root0(2), 1);
        assert_eq!(root0(3), 3);
        assert_eq!(root0(4), 3);
        assert_eq!(root0(5), 7);
        assert_eq!(root0(6), 7);
        assert_eq!(root0(7), 7);
        assert_eq!(root0(8), 7);
        assert_eq!(root0(9), 15);
        assert_eq!(root0(10), 15);
        assert_eq!(root0(11), 15);
        assert_eq!(root0(12), 15);
        assert_eq!(root0(13), 15);
        assert_eq!(root0(14), 15);
        assert_eq!(root0(15), 15);
        assert_eq!(root0(16), 15);
        assert_eq!(root0(17), 31);
        assert_eq!(root0(18), 31);
        assert_eq!(root0(19), 31);
        assert_eq!(root0(20), 31);
        assert_eq!(root0(21), 31);
        assert_eq!(root0(22), 31);
        assert_eq!(root0(23), 31);
        assert_eq!(root0(24), 31);
        assert_eq!(root0(25), 31);
        assert_eq!(root0(26), 31);
        assert_eq!(root0(27), 31);
        assert_eq!(root0(28), 31);
        assert_eq!(root0(29), 31);
        assert_eq!(root0(30), 31);
        assert_eq!(root0(31), 31);
        for i in 1..32 {
            println!("assert_eq!(root0({}),{});", i, root0(i))
        }
    }
}
