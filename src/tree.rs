use std::ops::Range;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(pub usize);

impl PartialEq<usize> for Offset {
    fn eq(&self, other: &usize) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<usize> for Offset {
    fn partial_cmp(&self, other: &usize) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

pub fn leafs(blocks: Offset) -> usize {
    (blocks.0 + 1) / 2
}

/// Root offset given a number of leaves.
pub fn root(leafs: usize) -> Offset {
    Offset(root0(leafs))
}

fn root0(leafs: usize) -> usize {
    leafs.next_power_of_two() - 1
}

/// Level for an offset. 0 is for leaves, 1 is for the first level of branches, etc.
pub fn level(offset: Offset) -> u32 {
    level0(offset.0)
}

fn level0(offset: usize) -> u32 {
    (!offset).trailing_zeros()
}

/// Span for an offset. 1 is for leaves, 2 is for the first level of branches, etc.
pub fn span(offset: Offset) -> Offset {
    Offset(span0(offset.0))
}

fn span0(offset: usize) -> usize {
    1 << (!offset).trailing_zeros()
}

pub fn left_child(offset: Offset) -> Option<Offset> {
    left_child0(offset.0).map(Offset)
}

fn left_child0(offset: usize) -> Option<usize> {
    let span = span0(offset);
    if span == 1 {
        None
    } else {
        Some(offset - span / 2)
    }
}

pub fn right_child(offset: Offset) -> Option<Offset> {
    right_child0(offset.0).map(Offset)
}

fn right_child0(offset: usize) -> Option<usize> {
    let span = span0(offset);
    if span == 1 {
        None
    } else {
        Some(offset + span / 2)
    }
}

/// Get a valid right descendant for an offset
pub fn right_descendant(offset: Offset, len: Offset) -> Option<Offset> {
    let mut offset = right_child(offset)?;
    while offset >= len {
        offset = left_child(offset)?;
    }
    Some(offset)
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
pub fn children(offset: Offset) -> Option<(Offset, Offset)> {
    let span = span(offset);
    if span.0 == 1 {
        None
    } else {
        Some((Offset(offset.0 - span.0 / 2), Offset(offset.0 + span.0 / 2)))
    }
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
pub fn descendants(offset: Offset, len: Offset) -> Option<(Offset, Offset)> {
    let lc = left_child(offset);
    let rc = right_descendant(offset, len);
    if let (Some(l), Some(r)) = (lc, rc) {
        Some((l, r))
    } else {
        None
    }
}

pub fn is_left_sibling(offset: Offset) -> bool {
    is_left_sibling0(offset.0)
}

fn is_left_sibling0(offset: usize) -> bool {
    let span = span0(offset) * 2;
    (offset & span) == 0
}

pub fn parent(offset: Offset) -> Offset {
    Offset(parent0(offset.0))
}

fn parent0(offset: usize) -> usize {
    let span = span0(offset);
    // if is_left_sibling(offset) {
    if (offset & (span * 2)) == 0 {
        offset + span
    } else {
        offset - span
    }
}

pub fn index(offset: Offset) -> usize {
    offset.0 / 2
}

pub fn range(offset: Offset) -> Range<Offset> {
    let r = range0(offset.0);
    Offset(r.start)..Offset(r.end)
}

fn range0(offset: usize) -> Range<usize> {
    let span = span0(offset);
    offset + 1 - span..offset + span
}

pub fn sibling(offset: Offset) -> Offset {
    Offset(sibling0(offset.0))
}

fn sibling0(offset: usize) -> usize {
    if is_left_sibling0(offset) {
        offset + span0(offset) * 2
    } else {
        offset - span0(offset) * 2
    }
}

/// depth first, left to right traversal of a tree of size len
pub fn depth_first_left_to_right(len: Offset) -> impl Iterator<Item = Offset> {
    fn descend(offset: Offset, len: Offset, res: &mut Vec<Offset>) {
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
    let mut res = Vec::with_capacity(len.0);
    descend(root, len, &mut res);
    res.into_iter()
}

/// breadth first, left to right traversal of a tree of size len
pub fn breadth_first_left_to_right(len: Offset) -> impl Iterator<Item = Offset> {
    fn descend(current: Vec<Offset>, len: Offset, res: &mut Vec<Offset>) {
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
    let mut res = Vec::with_capacity(len.0);
    descend(vec![root], len, &mut res);
    res.into_iter()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for Offset {
        type Parameters = ();
        type Strategy = BoxedStrategy<Offset>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            any::<usize>().prop_map(Offset).boxed()
        }
    }

    #[test]
    fn test_right_descendant() {
        for i in (1..11) {
            println!(
                "valid_right_child({}, 9), {:?}",
                i,
                right_descendant(Offset(i), Offset(9))
            );
        }
    }

    #[test]
    fn test_left() {
        for i in 0..20 {
            println!("assert_eq!(left_child({}), {:?})", i, left_child(Offset(i)));
        }
        for i in 0..20 {
            println!("assert_eq!(is_left({}), {})", i, is_left_sibling(Offset(i)));
        }
        for i in 0..20 {
            println!("assert_eq!(parent({}), {:?})", i, parent(Offset(i)));
        }
        for i in 0..20 {
            println!("assert_eq!(sibling({}), {:?})", i, sibling(Offset(i)));
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
        fn dflr(len: usize) -> impl Iterator<Item = Offset> {
            depth_first_left_to_right(Offset(len))
        }
        assert_eq!(dflr(1).collect::<Vec<_>>(), vec![0]);
        assert_eq!(dflr(3).collect::<Vec<_>>(), vec![1, 0, 2]);
        assert_eq!(dflr(5).collect::<Vec<_>>(), vec![3, 1, 0, 2, 4]);
        assert_eq!(dflr(7).collect::<Vec<_>>(), vec![3, 1, 0, 2, 5, 4, 6]);
        assert_eq!(dflr(9).collect::<Vec<_>>(), vec![7, 3, 1, 0, 2, 5, 4, 6, 8]);
    }

    #[test]
    fn test_bflr() {
        fn bflr(len: usize) -> impl Iterator<Item = Offset> {
            breadth_first_left_to_right(Offset(len))
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
