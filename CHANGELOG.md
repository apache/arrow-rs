For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)

# Changelog

## [4.1.0](https://github.com/apache/arrow-rs/tree/4.2.0) (2021-05-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.1.0...4.2.0)

**Merged pull requests:**

* 0c9e3c6a8656837398fddade01eac802868de743 inline PrimitiveArray::value (#329) (#354)
* 8cc9b711ab161b7505e868177f48289346663dc8 Fix filter UB and add fast path (#341) (#372)
* 58d53cfc8dcf018baf5e15097c3f8a402dc48ea1 Add crate badges (#362) (#373)
* f0702df314434a1c79184c019b09d2aa2c39c00f Only register Flight.proto with cargo if it exists (#351) (#374)
* 08177837ad766327b6213f1a14fa606727f510c4 Doctests for BooleanArray. (#338) (#359)
* 9e1505063954a41e64fc5a54b1dd1c216b491d7c respect offset in utf8 and list casts (#335) (#358)
* ceeab6cd082d1a46713a61bc225f81b68f959fa0 fix comparison of dictionaries with different values arrays (#332) (#333) (#357)
* 6ef14ce503f8bf67db32c8ecb15891317b73a8fa fix invalid null handling in filter (#296) (#356)
* 1bdfbd921ef0ff201e598ec54a6b0a84c4ffd1f2 Doctests for StringArray and LargeStringArray. (#330) (#355)
* 305b41e97692b0b342846c3bff06e42f439772fa Mutablebuffer::shrink_to_fit (#318) (#344)
