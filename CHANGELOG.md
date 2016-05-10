# Change Log
All notable changes to Gregor will be documented in this file.

## [0.3.0] - 2016-05-09

### Breaking Changes
- Changed the arity of `resume` and `pause` to align with the rest of the API
  (`assoc`-like optional arg pairs)

## Changes
- Change name of second `seek-to!` arg from `destination` to `offset`.

### Fixed
- Fixed `resume` and `pause` having the same implementation.
- Merge pull request from `lambdahands` which fixes issue w/ overwritten custom
  (de)serializers in producer and consumer configs.

## [0.2.0] - 2016-03-25

### Added
- First public release: added most of API
