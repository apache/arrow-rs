For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)

# Changelog

## [4.4.0](https://github.com/apache/arrow-rs/tree/4.4.0) (2021-06-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.3.0...4.4.0)

* 43b4f721fc31c7c6b700aea8bf4de8800a7f49bc implement second/minute helpers for temporal (#493) (#498)
* 4d3f826b622273003d133ae9ec481850fb5bb058 concatenating single element array shortcut (#492) (#497)
* eb1d1e158e2cde8c7f8efed5929d89803a12daa6 Doctest for GenericListArray. (#474) (#496)
* c0f06d4d1bc5a716c7cab168c97f7ecd65615bc7 Add C data interface for decimal128 and timestamp (#453) (#495)
* c1f9083ef6db9bdc3b3b6046284f0ca6be99c0ab Use partition for bool sort (#448) (#494)
* d851decbabd5ff5f72af08602830199404fb08b4 remove stale comment and update unit tests (#472) (#487)
* 1c3431a15bc7b3d57bb3399ecf8f2702dfcae459 Implement the Iterator trait for the json Reader. (#451) (#486)
* 638a605a356306ce82ba2ac0b66aee81251cdaad remove clippy unnecessary wraps (#449) (#485)
* 563a5067a9e722808a298817c153071fb51a6535 remove unnecessary wraps in sortk (#445) (#483)
* b5e50efa6d8d7499e7785b2862eb60e19770ea11 window::shift to work for all array types (#388) (#464)
* 93b51718671fecb9511b5eabf069940e07fa4b4c Add Decimal to CsvWriter and improve debug display (#406) (#465)
* 153085f526939e83f7cad8d38cbdd176728b62af Backport clippy fixes to active release (#475)


## [4.3.0](https://github.com/apache/arrow-rs/tree/4.3.0) (2021-06-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.2.0...4.3.0)

* a7656a8a3cd1f02e4543e1b971842ca92404f82a refactor lexico sort (#424) (#441)
* 714f124618c500e38d3198a40cb51514529a0184 refactor lexico sort (#423) (#442)
* 033dd4f5ab7403a1b0dfe19b95921da80e2047d3 Derive Eq and PartialEq for SortOptions (#425) (#433)
* f5aa7026f2ecdf30d256c0ab10a1159e40425191 Sort by float lists (#420) (#435)
* e64c9eb0eec3744655ce166146f3ee1930ff542e Fix bug with null buffer offset in boolean not kernel (#418) (#434)
* 5b8003553171a772c9106ea70e3095beef3363c9 Cherry pick Reduce memory usage of concat (large)utf8 to active_release (#411)
* 8179193a0f1cc4602d05e201b4f1ae16142eec10 Fix out of bounds read in bit chunk iterator (#416) (#432)
* 1327abebf0a326bcf062b11180256f6f8328b198 Add set_bit to BooleanBufferBuilder to allow mutating bit in index (#383) (#431)
* 59bd90ad4c78d3fd07bec29f52d06345e563b923 Respect max rowgroup size in Arrow writer (#381) (#430)
* 7f7d71464d022d2049c5bb2f0867db2472a597dd add more tests for window::shift and handle boundary cases (#386) (#429)
* e3d679222c4aa4dfcfdff7459033b81a57c85ed0 Simplify window using null array (#370) (#402)
* a906dbe46a69d574afc0accec98c36deadb988ff Doctests for FixedSizeBinaryArray (#378) (#403)
* 4d216f33f9d51a86570e9d17d175f8aec5127c44 ensure null-counts are written for all-null columns (#307) (#404)
* c928d57d80e99e35789ea20f02e30d7dbc98aa77 allow to read non-standard CSV (#326) (#407)
* db581f3b9f80f8d620489a79d51347cf5af13b9c parquet: Speed up `BitReader`/`DeltaBitPackDecoder` (#325) (#408)
* 8059cf5800ad4be917ac0264e70bad776598c739 Add (simd) modulus op (#317) (#410)
* 07d00c0f3706e33e9c37feab91b8f60333b75e9c Add doctest for ArrayBuilder (#367) (#412)
* 84fed05befb50e5d0d6ba4d1ee09cdc1ad4d7a37 allow `SliceableCursor` to be constructed from an `Arc` directly (#369) (#401)


## [4.2.0](https://github.com/apache/arrow-rs/tree/4.2.0) (2021-05-29)

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


## [4.1.0](https://github.com/apache/arrow-rs/tree/4.1.0) (2021-05-17)

[Full Changelog](https://github.com/apache/arrow-rs/compare/4.0.0...4.1.0)

a5dd428f57e62db20a945e8b1895de91405958c4 Add changelog and bump version for proposed 4.1.0 initial bi-weekly release #298 (#305)
* c863a2c44bffa5c092a49e07910d5e9225483193 Fix FFI and add support for Struct type (#287)
* de44c8cd136c212628671a288ddf64a5c88a92a4 Added changelog generator script and configuration. (#289)
* e7d5efe58d1f94e0309149495dfa433b6778c259 Add Send to the ArrayBuilder trait (#291)
* a959c85f8e567e7f117445f78a7c524e57edfaf4 Version upgrades (#304)
* 1fc48daab0d915fcd26c919d2fc47a7b057bd13e Remove old release scripts (#293)
* 3665296b4e985a840ad21815b839e5fa3fc462d5 manually bump development version (#288)
* 4449ee96fe3fd4a0b275da8dd25ce2792699bc98 Added Decimal support to pretty-print display utility (#230) (#273)
* ce8e67c28ad1431cda36b38434e53871c2dd520a Fix subtraction underflow when sorting string arrays with many nulls (#285)
* 8226219fe7104f6c8a2740806f96f02c960d991c Fix null struct and list roundtrip (#270)
* 510f02f449193bea9df3f423d18ce7a9e4112bdf Speed up bound checking in `take` (#281)
* aba044f8294498f2100532b08d433f848d9d2a29 Update PR template by commenting out instructions (#278)
* 2f5f58a2087be67eeeeb0109f0c8843c216a4fd1 support full u32 and u64 roundtrip through parquet (#258)
* 8bd769bc118f617580cfaa7b1e654159c0fb696b 1.52 clippy fixes (#267)
* a870b24bd4eb76d3e0e5c718c9956a7dcdee52fd Fix typo in csv/reader.rs (#265)
* 64ea8dae64b05a1a4ffcde739b02411219653dc2 Fix empty Schema::metadata deserialization error (#260)
* b76e8c9fa3c6373dd839d2547a5c010f4a31ecae update datafusion and ballista links (#259)
* 508f25c10032857da34ea88cc8166f0741616a32 Added env to run rust in integration. (#253)
* 04779e0b57efa2f88c75abc080cd5feb70737484 fix NaN handling in parquet statistics (#256)
* 6a6554361d5ac2304b17723e00910ccea34a710a fix parquet max_definition for non-null structs (#246)
* 8f030db53d9eda901c82db9daf94339fc447d0db Made integration tests always run. (#248)
* a040c15fb6eb7353426b0cccaf56c64832121498 Improve docs for NullArray, new_null_array and new_empty_array (#240)
* 4dfbca6e5791be400d2fd3ae863655445327650e sort_primitive result is capped to the min of limit or values.len (#236)
* 486524733639d3c9e60e44bb07a65c628958b7b6 Disabled rebase needed until demonstrate working. (#243)
* d008f31b107c1030a1f5144c164e8ca8bf543576 pin flatbuffers to 0.8.4 (#239)
* 2121150a0d5536865f4acdf8ee440b900d236e06 [Parquet] Read list field correctly (#234)
* ed00e4d4a160cd5182bfafb81fee2240ec005014 Fix code examples for RecordBatch::try_from_iter (#231)
* 111d5d67c76500b14aece28b3cbf145c4400f7fb Support string dictionaries in csv reader (#228) (#229)
* 51513c165b8f39760960f2eeac9262f0b6e8c9f1 ARROW-12411: [Rust] Create RecordBatches from Iterators (#7)
* 20f6c7e26d14d6df461d17c95d9a254c71bb0c72 support LargeUtf8 in sort kernel (#26)
* 2214263a6a0df2b2fc1c8a2540d7a9f022bbc110 Removed unused files (#22)
* 463f88f5553a51a0498658bdc5a0ba13a2e24eda Support auto-vectorization for min/max using multiversion (#9)
* 861b5723cff4bed0a03dda6102fe02d7c09618a5 Add GitHub templates (#17)
* a0d5e11d2938312ae5856be8cba8f9ae798cab03 Added rebase-needed bot (#13)
* c3fe3bab9905739fdda75301dab07a18c91731bd Buffer::from_slice_ref set correct capacity (#18)
* 90d4f1d33e6644554c4db4592b8fa45f7c209356 ARROW-12493: Add support for writing dictionary arrays to CSV and JSON (#16)
* a5732e80508c60cb65cc049c57f8bca5d38fc1c4 ARROW-12426: [Rust] Fix concatentation of arrow dictionaries (#15)
* 74d3567277c79dea74ec1c10411fd3727bd512bc Added Integration tests against arrow (#10)
* 8c1063c7b5e0c308ff5bcc4ba0283a2f0d67eeec Update URLs (#14)
* 3d7cefb41db55e7b3d0735a12b4cd2945a16f965 Fixed labeler. (#8)
* 5918670a498a6514866e97b5d8ff75b494ef0ede Allow creating issues. (#6)
* 08812b915b8a53197754bfe80e7ccbaed2d3fd39 Removed bot comment about title and JIRA. (#4)
* 8707fd2b2d17b17bd3e79be0255a18ffaea6914a Made CI run on any change. (#5)
* 72eda5a255182455651cd45a20ed847756a2a9aa Fixed ident error.
* a7d71ad68c04e115328001d8abe4552a0177bc5a Specify GitHub settings (#2)
* 6b150f283551eef02b758700682c52c92124d916 Temporarily removed integration in CI.
* bbc523c952a523341b9c18316d69ed41524aa1cf Removed unused cancel jobs
* 6f33ba245092d03dd6120e6e97f39ff16b4afc46 Fixed Linting.
* fd8bb74bd4c60d2a2bab79639660216794238830 Trying dev fix.
* 2b2d608f45769e8e30647c6383245b061d1ca6d9 Fiddle paths for testing.
* 99cef578854a07f47958801bdddc5c282b9d6808 Removed test from datafusion.
* f1862ed5fb537c8a315140107b9787a070278fe7 Fiddle with CI paths
* f6d45c42287b32f6a270d10db204e81dae4012cb Fiddle paths to submodules.
* 444b1ca177a035f827b461541ed8e68ab31daf6f Fixed CI.
* 53c9668a5154e19299315564301f3a92f9f4554e Fixed RAT and Rust linting.
* d30eed35aa102754f4c61eeaeda530c0149fe257 Update paths to arrow and parquet test data
* a98e2753cc86a66b9658790fc0951e60064a8d34 Add parquet-testing submodule
* 9f9c58c79da4cdc6175cda5ea5edba6385003141 Remove submodule
* add5261f169438583ce09ef3eb0613b051e9c470 Fiddle path to flight protobuf generation.
* cf0c7d2cbecc2ca6ceeb7459417cef3501f64ad1 Flatten directory.
* c35720914fd3658580ba65152ac0eac5822fc481 Removed unused files.
* 5a4574b89b2ab2d1ca5aa8831c8625fd27520e56 Removed DataFusion and Ballista.
* f55fce1b16f82fc4ba4c5b6321b1bf2989978f6d Changed references to DF and Ballista in Cargo.
