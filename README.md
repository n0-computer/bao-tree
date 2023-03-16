# bao-tree

The merkle tree used for blake3 verified streaming.

This is a slightly different take on blake3 verified streaming than the 
[bao](https://github.com/oconnor663/bao) crate.

The network wire format for encoded data and slices is compatible with the bao
crate, except that this crate has builtin support for *runtime* configurable chunk
groups.

The intention is also to support both sync and async en/decoding out of the box
with maximum code sharing.

It also allows encoding not just single ranges but sets of non-overlapping ranges.
E.g. you can ask for bytes `[0..1000,5000..6000]` in a single query.

It allows to define both pre- and post order outboard formats. Post order outboard
formats have advantages for synchronizing append only files.