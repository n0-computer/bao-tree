# Changelog

All notable changes to bao-tree will be documented in this file.

### ⚠️ Breaking Changes

- The serde impl of `Parent` wrote a sequence length of 2 for its 3 elements, so length prefixed formats like postcard could not deserialize parents. It now writes 3, which changes the wire format of `experimental-mixed` ([#103](https://github.com/n0-computer/bao-tree/issues/103))
- The io error inside `EncodeError` is serialized as a kind number and a message instead of a string, so the kind survives a round trip. This changes the wire format ([#109](https://github.com/n0-computer/bao-tree/issues/109))
- `BlockSize` is limited to chunk groups of 64 MiB (`BlockSize::MAX`). `from_chunk_log` panics above it and `from_bytes` returns `None` ([#96](https://github.com/n0-computer/bao-tree/issues/96))
- The mixed traversal emits the real tree node for parents inside chunk groups instead of `TreeNode(0)` ([#100](https://github.com/n0-computer/bao-tree/issues/100))
- The `experimental-mixed` feature enables the `serde` feature, without which it did not compile ([#100](https://github.com/n0-computer/bao-tree/issues/100))
- The minimum supported Rust version is 1.85 ([#109](https://github.com/n0-computer/bao-tree/issues/109))

### ⛰️ Features

- Add `keyed_encode_ranges` for sync and fsm ([#83](https://github.com/n0-computer/bao-tree/issues/83))

### 🐛 Bug Fixes

- Fix encoding and decoding of empty ranges, so the fsm and sync encoders agree and both decoders accept an empty response ([#82](https://github.com/n0-computer/bao-tree/issues/82))
- Fix `encode_ranges` for partial chunk groups, which were written without the inner parent pairs the decoder expects ([#83](https://github.com/n0-computer/bao-tree/issues/83))
- Stop decoding after the first error, instead of verifying further items against unauthenticated hashes ([#84](https://github.com/n0-computer/bao-tree/issues/84))
- `ResponseDecoder::hash` returns the root hash during decoding instead of the bottom of the hash stack ([#85](https://github.com/n0-computer/bao-tree/issues/85))
- Return an error instead of panicking when an outboard has no hash pair for a node ([#87](https://github.com/n0-computer/bao-tree/issues/87))
- Ignore saves for nodes an outboard does not store, so `decode_ranges` into mem outboards works for partial chunk groups ([#94](https://github.com/n0-computer/bao-tree/issues/94))
- Fix overflows for trees and ranges near `u64::MAX` ([#95](https://github.com/n0-computer/bao-tree/issues/95))
- Return zero hashes past the end of sync file outboards, like the fsm outboards ([#97](https://github.com/n0-computer/bao-tree/issues/97))
- Treat data past the end of the file as invalid in `valid_ranges` instead of failing ([#98](https://github.com/n0-computer/bao-tree/issues/98))
- Yield nothing for empty ranges in `valid_ranges` and `valid_outboard_ranges` for trees with a single chunk group ([#99](https://github.com/n0-computer/bao-tree/issues/99))
- Report write errors of the sync encoders as `ParentWrite` or `LeafWrite`, like the fsm encoders ([#103](https://github.com/n0-computer/bao-tree/issues/103))
- Fix docs and declare the features the examples need ([#107](https://github.com/n0-computer/bao-tree/issues/107))

### 🚜 Refactor

- Deprecate `BaoTree::root`, which ignores the block size ([#105](https://github.com/n0-computer/bao-tree/issues/105))
- Remove dead code ([#104](https://github.com/n0-computer/bao-tree/issues/104))

### 🧪 Testing

- Test matrix for the io implementations, error handling tests and coverage measurement ([#101](https://github.com/n0-computer/bao-tree/issues/101), [#102](https://github.com/n0-computer/bao-tree/issues/102), [#103](https://github.com/n0-computer/bao-tree/issues/103), [#106](https://github.com/n0-computer/bao-tree/issues/106))

### ⚙️ Miscellaneous Tasks

- CI hardening, lock files and dependabot cooldown ([#86](https://github.com/n0-computer/bao-tree/issues/86))
- Make the wasm CI check build a wasm file ([#108](https://github.com/n0-computer/bao-tree/issues/108))
