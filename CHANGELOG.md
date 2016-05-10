# Change Log
All notable changes to Gregor will be documented in this file.

## [0.3.0] - 2016-05-09

Apologies for the several (pretty minor) breaking changes.

### Breaking Changes
- Arity of `resume` and `pause` now aligns with the rest of the API (`assoc`-like
  optional arg pairs)
- `commit-offsets-async!` and `commit-offsets!` optional arg `offsets` is now a seq of
  maps with `:topic`, `:partition`, `:offset` and optional `:metadata` keys.
- `commited` now returns `nil` or a map with `:offset` and `:metadata` keys.
- `send` no longer supports a callback, use `send-then` instead.

### Changes
- Second `seek-to!` arg is now named `offset`.
- `send` has new arities that correspond to those of the `ProducerRecord` constructor.

### Fixed
- `resume` and `pause` no longer have the same implementation.
- Merge pull request from `lambdahands` which fixes issue w/ overwritten custom
  (de)serializers in producer and consumer configs.

### Added
- `send-then` function which provides `send` a callback. This callback expects a map of
  metadata and an exception as its args.
- `->producer-record` function.


## [0.2.0] - 2016-03-25

### Added
- First public release: added most of API
