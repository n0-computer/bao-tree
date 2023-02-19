# bao-experiment
Attempt to come up with a new outboard format for bao

This outboard format stores the merkle tree nodes in order, instead of post order like the original bao. This has the advantage that you can reuse the
outboard when adding data. The downside is that data in the outboard is not ordered in the right way, so it requires more seeking when computing a slice.

The wire format when transferring slices is identical to bao.
