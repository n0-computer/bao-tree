use std::ops::Range;

use crate::tree::ChunkNum;

/// A range specification
///
/// Basically just a wrapper around a strictly sorted sequence of ChunkNums
pub struct RangeSpec<'a>(&'a [ChunkNum]);

impl<'a> RangeSpec<'a> {
    pub fn new(ranges: &'a [ChunkNum]) -> Option<RangeSpec<'a>> {
        for i in 0..ranges.len().saturating_sub(1) {
            if ranges[i] >= ranges[i + 1] {
                return None;
            }
        }
        Some(RangeSpec(ranges))
    }

    pub fn split(&self, at: ChunkNum) -> (RangeSpec<'a>, RangeSpec<'a>) {
        let (left, right) = split(self.0, at);
        (RangeSpec(left), RangeSpec(right))
    }

    pub fn contains(&self, chunk: ChunkNum) -> bool {
        contains(self.0, &chunk)
    }

    pub fn intersects(&self, range: Range<ChunkNum>) -> bool {
        intersects(self.0, range)
    }
}

#[inline]
fn is_odd(x: usize) -> bool {
    (x & 1) != 0
}

#[inline]
fn is_even(x: usize) -> bool {
    (x & 1) == 0
}

/// Split a strictly ordered sequence of boundaries `ranges` into two parts
/// `left`, `right` at position `at`, so that
///   contains(left, x) == contains(ranges, x) for x < at
///   contains(right, x) == contains(ranges, x) for x >= at
#[inline]
fn split<'a, T: Ord>(ranges: &'a [T], at: T) -> (&'a [T], &'a [T]) {
    let l = ranges.len();
    let res = ranges.binary_search(&at);
    match res {
        Ok(i) if is_even(i) => {
            // left will be an even size, so we can just cut it off
            (&ranges[..i], &ranges[i..])
        }
        Err(i) if is_even(i) => {
            // right will be an even size, so we can just cut it off
            (&ranges[..i], &ranges[i..])
        }
        Ok(i) => {
            // left will be an odd size, so we need to add one if possible
            //
            // since i is an odd value, it indicates going to false at the
            // split point, and we don't need to have it in right.
            let sp = i.saturating_add(1).min(l);
            (&ranges[..sp], &ranges[sp..])
        }
        Err(i) => {
            // left will be an odd size, so we add one if possible
            //
            // i is an odd value, so right is true at the split point, and
            // we need to add one value before the split point to right.
            // hence the saturating_sub(1).
            (
                &ranges[..i.saturating_add(1).min(l)],
                &ranges[i.saturating_sub(1)..],
            )
        }
    }
}

/// For a strictly ordered sequence of boundaries `ranges`, checks if the
/// value at `at` is true.
fn contains<T: Ord>(boundaries: &[T], value: &T) -> bool {
    match boundaries.binary_search(value) {
        Ok(index) => !is_odd(index),
        Err(index) => is_odd(index),
    }
}

/// Check if a sequence of boundaries `ranges` intersects with a range
fn intersects<T: Ord>(boundaries: &[T], range: Range<T>) -> bool {
    let (_, remaining) = split(boundaries, range.start);
    let (remaining, _) = split(remaining, range.end);
    !remaining.is_empty()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, ops::Range};

    use super::*;
    use proptest::prelude::*;

    fn test_points(boundaries: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
        let mut res = BTreeSet::new();
        for x in boundaries {
            res.insert(x.saturating_sub(1));
            res.insert(x);
            res.insert(x.saturating_add(1));
        }
        res
    }

    fn test_boundaries() -> impl Strategy<Value = (Vec<u64>, u64)> {
        proptest::collection::vec(any::<u64>(), 0..100).prop_flat_map(|mut v| {
            v.sort();
            v.dedup();
            // split point should occasionally be outside of the range
            let max_split = v
                .iter()
                .max()
                .cloned()
                .unwrap_or_default()
                .saturating_add(2);
            (Just(v), 0..max_split)
        })
    }

    proptest! {
        #[test]
        fn test_split((boundaries, at) in test_boundaries()) {
            let (left, right) = split(&boundaries, at);
            for x in test_points(boundaries.clone()) {
                // test that split does what it promises
                if x < at {
                    prop_assert_eq!(contains(left, &x), contains(&boundaries, &x), "left must be like boundaries for x < at");
                } else {
                    prop_assert_eq!(contains(right, &x), contains(&boundaries, &x), "right must be like boundaries for x >= at");
                }
                // test that split is not just returning the input, but minimal parts
                let nr = right.iter().filter(|x| x < &&at).count();
                prop_assert!(nr <= 1, "there must be at most one boundary before the split point");
                let nl = left.iter().filter(|x| x >= &&at).count();
                prop_assert!(nl <= 1, "there must be at most one boundary after the split point");
            }
        }
    }

    #[test]
    fn test_split_0() {
        let cases: Vec<(&[u64], u64, (&[u64], &[u64]))> = vec![
            (&[0, 2], 0, (&[], &[0, 2])),
            (&[0, 2], 2, (&[0, 2], &[])),
            (&[0, 2, 4], 2, (&[0, 2], &[4])),
            (&[0, 2, 4], 4, (&[0, 2], &[4])),
            (&[0, 2, 4, 8], 2, (&[0, 2], &[4, 8])),
            (&[0, 2, 4, 8], 4, (&[0, 2], &[4, 8])),
            (&[0, 2, 4, 8], 3, (&[0, 2], &[4, 8])),
            (&[0, 2, 4, 8], 6, (&[0, 2, 4, 8], &[4, 8])),
        ];
        for (ranges, pos, (left, right)) in cases {
            assert_eq!(split(&ranges, pos), (left, right));
        }
    }

    #[test]
    fn test_intersects_0() {
        let cases: Vec<(&[u64], Range<u64>, bool)> = vec![
            (&[0, 2], 0..2, true),
            (&[0, 2], 2..4, false),
            (&[0, 2, 4, 8], 0..2, true),
            (&[0, 2, 4, 8], 2..4, false),
            (&[0, 2, 4, 8], 4..8, true),
            (&[0, 2, 4, 8], 8..12, false),
        ];
        for (ranges, range, expected) in cases {
            assert_eq!(intersects(&ranges, range), expected);
        }
    }

    #[test]
    fn contains_0() {
        let cases: Vec<(&[u64], u64, bool)> = vec![
            (&[0, 2], 0, true),
            (&[0, 2], 1, true),
            (&[0, 2], 2, false),
            (&[0, 2, 4, 8], 3, false),
            (&[0, 2, 4, 8], 4, true),
        ];
        for (ranges, pos, expected) in cases {
            assert_eq!(contains(ranges, &pos), expected);
        }
    }
}
