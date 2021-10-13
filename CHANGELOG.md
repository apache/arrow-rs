For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)

# Changelog

## [6.0.0](https://github.com/apache/arrow-rs/tree/6.0.0) (2021-10-13)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.5.0...6.0.0)

**Breaking changes:**

- Replace `ArrayData::new()` with `ArrayData::try_new()` and `unsafe ArrayData::new_unchecked` [\#822](https://github.com/apache/arrow-rs/pull/822) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Update Bitmap::len to return bits rather than bytes [\#749](https://github.com/apache/arrow-rs/pull/749) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- use sort\_unstable\_by in primitive sorting [\#552](https://github.com/apache/arrow-rs/pull/552) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- refactor: remove lifetime from DynComparator [\#542](https://github.com/apache/arrow-rs/pull/542) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([e-dard](https://github.com/e-dard))
- Minimal MapArray support [\#491](https://github.com/apache/arrow-rs/pull/491) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nevi-me](https://github.com/nevi-me))
- use iterator for partition kernel instead of generating vec [\#438](https://github.com/apache/arrow-rs/pull/438) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Remove DictionaryArray::keys\_array method [\#419](https://github.com/apache/arrow-rs/pull/419) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- simplify interactions with arrow flight APIs [\#377](https://github.com/apache/arrow-rs/pull/377) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([garyanaplan](https://github.com/garyanaplan))
- return reference from DictionaryArray::values\(\) \(\#313\) [\#314](https://github.com/apache/arrow-rs/pull/314) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))

**Implemented enhancements:**

- Improve parquet binary writer speed by reducing allocations [\#819](https://github.com/apache/arrow-rs/issues/819)
- Expose buffer operations [\#808](https://github.com/apache/arrow-rs/issues/808)
- Add doc examples of writing parquet files using `ArrowWriter` [\#788](https://github.com/apache/arrow-rs/issues/788)
- generate parquet schema from rust struct [\#539](https://github.com/apache/arrow-rs/pull/539) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Implement `RecordBatch::concat` [\#537](https://github.com/apache/arrow-rs/pull/537) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Implement function slice for RecordBatch [\#490](https://github.com/apache/arrow-rs/pull/490) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([b41sh](https://github.com/b41sh))
- add lexicographically partition points and ranges [\#424](https://github.com/apache/arrow-rs/pull/424) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- allow to read non-standard CSV [\#326](https://github.com/apache/arrow-rs/pull/326) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kazuk](https://github.com/kazuk))
- parquet: Speed up `BitReader`/`DeltaBitPackDecoder` [\#325](https://github.com/apache/arrow-rs/pull/325) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kornholi](https://github.com/kornholi))
- ARROW-12343: \[Rust\] Support auto-vectorization for min/max [\#9](https://github.com/apache/arrow-rs/pull/9) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- ARROW-12411: \[Rust\] Create RecordBatches from Iterators [\#7](https://github.com/apache/arrow-rs/pull/7) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Fixed bugs:**

- JSON reader can create null struct children on empty lists [\#825](https://github.com/apache/arrow-rs/issues/825)
- Incorrect null count for cast kernel for list arrays [\#815](https://github.com/apache/arrow-rs/issues/815)
- `minute` and `second` temporal kernels do not respect timezone [\#500](https://github.com/apache/arrow-rs/issues/500)
- Fix data corruption in json decoder f64-to-i64 cast [\#652](https://github.com/apache/arrow-rs/pull/652) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([xianwill](https://github.com/xianwill))
- fix take kernel null handling on structs [\#531](https://github.com/apache/arrow-rs/pull/531) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- Correct array memory usage calculation for dictionary arrays [\#505](https://github.com/apache/arrow-rs/pull/505) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- parquet: improve BOOLEAN writing logic and report error on encoding fail [\#443](https://github.com/apache/arrow-rs/pull/443) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([garyanaplan](https://github.com/garyanaplan))
- Fix bug with null buffer offset in boolean not kernel [\#418](https://github.com/apache/arrow-rs/pull/418) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- respect offset in utf8 and list casts [\#335](https://github.com/apache/arrow-rs/pull/335) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Fix comparison of dictionaries with different values arrays \(\#332\) [\#333](https://github.com/apache/arrow-rs/pull/333) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ensure null-counts are written for all-null columns [\#307](https://github.com/apache/arrow-rs/pull/307) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- fix invalid null handling in filter [\#296](https://github.com/apache/arrow-rs/pull/296) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- fix NaN handling in parquet statistics [\#256](https://github.com/apache/arrow-rs/pull/256) ([crepererum](https://github.com/crepererum))

**Documentation updates:**

- \[nit\] update readme.md and reformat [\#821](https://github.com/apache/arrow-rs/pull/821) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Doctest for PrimitiveArray using from\_iter\_values. [\#694](https://github.com/apache/arrow-rs/pull/694) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Update readme of release candidate verifying script. [\#648](https://github.com/apache/arrow-rs/pull/648) ([waynexia](https://github.com/waynexia))
- Add a note about arrow crate security / safety [\#628](https://github.com/apache/arrow-rs/pull/628) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Doctests for BinaryArray and LargeBinaryArray. [\#625](https://github.com/apache/arrow-rs/pull/625) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Add links in docstrings [\#605](https://github.com/apache/arrow-rs/pull/605) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Clean up README.md in advance of the 5.0 release [\#536](https://github.com/apache/arrow-rs/pull/536) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- fix readme instructions to reflect new structure [\#524](https://github.com/apache/arrow-rs/pull/524) ([marcvanheerden](https://github.com/marcvanheerden))
- Improve docs for NullArray, new\_null\_array and new\_empty\_array [\#240](https://github.com/apache/arrow-rs/pull/240) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))

**Merged pull requests:**

- JSON reader - empty nested list should not create child value [\#826](https://github.com/apache/arrow-rs/pull/826) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nevi-me](https://github.com/nevi-me))
- Add support for parsing timezone using chrono-tz [\#824](https://github.com/apache/arrow-rs/pull/824) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))
- Fewer ByteArray allocations when writing binary columns [\#820](https://github.com/apache/arrow-rs/pull/820) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Separate parquet writer benchmarks [\#818](https://github.com/apache/arrow-rs/pull/818) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix null count when casting ListArray [\#816](https://github.com/apache/arrow-rs/pull/816) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Make buffer::ops `pub` [\#809](https://github.com/apache/arrow-rs/pull/809) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- Doctests for DictionaryArrays. [\#805](https://github.com/apache/arrow-rs/pull/805) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Make parquet's optional arrow dependency skip the default features [\#801](https://github.com/apache/arrow-rs/pull/801) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([msalib](https://github.com/msalib))
- parquet: Avoid NaN check for non-floats [\#798](https://github.com/apache/arrow-rs/pull/798) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([kornholi](https://github.com/kornholi))
- Add Parquet writer example to docs [\#797](https://github.com/apache/arrow-rs/pull/797) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([matthewmturner](https://github.com/matthewmturner))
- add wasm32 to hash, fix wasm32 build [\#787](https://github.com/apache/arrow-rs/pull/787) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([seddonm1](https://github.com/seddonm1))
- Doctests for arrays - via collect method. [\#785](https://github.com/apache/arrow-rs/pull/785) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Make BooleanBufferBuilder get\_bit not require mutable reference [\#784](https://github.com/apache/arrow-rs/pull/784) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([boazberman](https://github.com/boazberman))
- fix: nanosecond timestamp scaling during string conversion \(\#780\) [\#781](https://github.com/apache/arrow-rs/pull/781) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ilya-biryukov](https://github.com/ilya-biryukov))
- handle tz while extractiing second/minute/hour from Timestamp\* arrays [\#771](https://github.com/apache/arrow-rs/pull/771) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))
- Add support for riscv64 [\#769](https://github.com/apache/arrow-rs/pull/769) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([felixonmars](https://github.com/felixonmars))
- Export `RowColumnIter` to fix doc [\#763](https://github.com/apache/arrow-rs/pull/763) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([huachaohuang](https://github.com/huachaohuang))
- Added PartialEq to RecordBatch [\#750](https://github.com/apache/arrow-rs/pull/750) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([matthewmturner](https://github.com/matthewmturner))
- Upgrade lexical-core to 0.8 [\#748](https://github.com/apache/arrow-rs/pull/748) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- fix: Support length on slices with null [\#745](https://github.com/apache/arrow-rs/pull/745) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- fix: Scalar math operations on slices [\#743](https://github.com/apache/arrow-rs/pull/743) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- fix: Comparisons against scalar slices [\#741](https://github.com/apache/arrow-rs/pull/741) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- fix: Handle slices in unary kernel [\#739](https://github.com/apache/arrow-rs/pull/739) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- Remove optional prettytable-rs dependency [\#737](https://github.com/apache/arrow-rs/pull/737) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszucs](https://github.com/kszucs))
- fix: new\_null\_array for structs [\#736](https://github.com/apache/arrow-rs/pull/736) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- fix: Allow parquet to be compiled without arrow \(fix --no-default-features\) [\#731](https://github.com/apache/arrow-rs/pull/731) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([Marwes](https://github.com/Marwes))
- Add `append_nulls` and `append_trusted_len_iter` to `PrimitiveBuilder` [\#728](https://github.com/apache/arrow-rs/pull/728) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([bjchambers](https://github.com/bjchambers))
- Add a note on rust compiler testing and compatibility [\#726](https://github.com/apache/arrow-rs/pull/726) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix decimal value\_as\_string [\#722](https://github.com/apache/arrow-rs/pull/722) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sergiimk](https://github.com/sergiimk))
- Fix decimal repr in parquet schema printer [\#721](https://github.com/apache/arrow-rs/pull/721) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([sergiimk](https://github.com/sergiimk))
- Optimize array::transform::utils::set\_bits [\#716](https://github.com/apache/arrow-rs/pull/716) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mathiaspeters-sig](https://github.com/mathiaspeters-sig))
- chore: Reduce the amount of code generated by monomorphization [\#715](https://github.com/apache/arrow-rs/pull/715) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Marwes](https://github.com/Marwes))
- run results of `cargo --fix` for edition 2021 [\#714](https://github.com/apache/arrow-rs/pull/714) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Jimexist](https://github.com/Jimexist))
- Parquet Derive: remove obscure feature flags, make chrono time emit converted type [\#712](https://github.com/apache/arrow-rs/pull/712) ([xrl](https://github.com/xrl))
- Support arrow readers for strings with DELTA\_BYTE\_ARRAY encoding [\#709](https://github.com/apache/arrow-rs/pull/709) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([ilya-biryukov](https://github.com/ilya-biryukov))
- Implement `regexp_matches_utf8` [\#706](https://github.com/apache/arrow-rs/pull/706) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([b41sh](https://github.com/b41sh))
- Support binary data type in `build_struct_array`. [\#702](https://github.com/apache/arrow-rs/pull/702) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([zijie0](https://github.com/zijie0))
- Add get\_bit to BooleanBufferBuilder [\#693](https://github.com/apache/arrow-rs/pull/693) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([boazberman](https://github.com/boazberman))
- PyO3 bridge for pyarrow interoperability / fix arrow integration test [\#691](https://github.com/apache/arrow-rs/pull/691) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszucs](https://github.com/kszucs))
- Update dev README with fancier regular expression for maintenance release notes [\#687](https://github.com/apache/arrow-rs/pull/687) ([alamb](https://github.com/alamb))
- Allow creation of String arrays from &Option\<&str\> iterators [\#680](https://github.com/apache/arrow-rs/pull/680) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([koomen](https://github.com/koomen))
- Make rand an optional dependency [\#674](https://github.com/apache/arrow-rs/pull/674) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([roee88](https://github.com/roee88))
- Doctests for DictionaryArray::from\_iter, PrimitiveDictionaryBuilder and DecimalBuilder. [\#673](https://github.com/apache/arrow-rs/pull/673) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Tiny tweaks to release readme [\#670](https://github.com/apache/arrow-rs/pull/670) ([alamb](https://github.com/alamb))
- Allow casting Timestamp arrays into String [\#664](https://github.com/apache/arrow-rs/pull/664) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([sum12](https://github.com/sum12))
- Add some doc comments to parquet bit\_util [\#663](https://github.com/apache/arrow-rs/pull/663) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Write FixedLenByteArray stats for FixedLenByteArray columns \(not ByteArray stats\) [\#662](https://github.com/apache/arrow-rs/pull/662) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Write boolean stats for boolean columns \(not i32 stats\) [\#661](https://github.com/apache/arrow-rs/pull/661) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- allocate enough bytes when writing booleans in parquet writer [\#658](https://github.com/apache/arrow-rs/pull/658) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([bjchambers](https://github.com/bjchambers))
- Change to comfy-table from prettytable-rs [\#656](https://github.com/apache/arrow-rs/pull/656) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([PsiACE](https://github.com/PsiACE))
- Add question template [\#649](https://github.com/apache/arrow-rs/pull/649) ([waynexia](https://github.com/waynexia))
- Doctests for from\_iter for BooleanArray & for BooleanBuilder. [\#647](https://github.com/apache/arrow-rs/pull/647) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Remove undefined behavior in `value` method of boolean and primitive arrays [\#644](https://github.com/apache/arrow-rs/pull/644) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Fix parquet string statistics generation [\#643](https://github.com/apache/arrow-rs/pull/643) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Add human readable Format for parquet ByteArray [\#642](https://github.com/apache/arrow-rs/pull/642) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([alamb](https://github.com/alamb))
- Speed up filter\_record\_batch with one array [\#637](https://github.com/apache/arrow-rs/pull/637) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Dandandan](https://github.com/Dandandan))
- Fix clippy lints for Rust 1.54 [\#631](https://github.com/apache/arrow-rs/pull/631) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- disallow bare trait objects [\#615](https://github.com/apache/arrow-rs/pull/615) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- resolve unnecessary borrow clippy lints [\#613](https://github.com/apache/arrow-rs/pull/613) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Jimexist](https://github.com/Jimexist))
- make FFI\_ArrowArray::empty\(\) public [\#612](https://github.com/apache/arrow-rs/pull/612) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wangfenjin](https://github.com/wangfenjin))
- Fix typo [\#604](https://github.com/apache/arrow-rs/pull/604) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([wangfenjin](https://github.com/wangfenjin))
- Doctests for UnionBuilder and UnionArray. [\#603](https://github.com/apache/arrow-rs/pull/603) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Implement `StringBuilder::append_option` [\#601](https://github.com/apache/arrow-rs/pull/601) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([mzeitlin11](https://github.com/mzeitlin11))
- fix dyn syntax for trait objects [\#592](https://github.com/apache/arrow-rs/pull/592) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- Remove Git SHA from created\_by Parquet file metadata [\#590](https://github.com/apache/arrow-rs/pull/590) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([carols10cents](https://github.com/carols10cents))
- Fix newline in changelog [\#588](https://github.com/apache/arrow-rs/pull/588) ([nealrichardson](https://github.com/nealrichardson))
- use exponential search to speed up lexico partition [\#585](https://github.com/apache/arrow-rs/pull/585) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- fix: undefined behavior in schema ffi [\#582](https://github.com/apache/arrow-rs/pull/582) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([roee88](https://github.com/roee88))
- support struct array in pretty display [\#579](https://github.com/apache/arrow-rs/pull/579) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([houqp](https://github.com/houqp))
- Update readme link to point at the right codecoverage location [\#577](https://github.com/apache/arrow-rs/pull/577) ([alamb](https://github.com/alamb))
- Update sort kernel docs to note it is unstable [\#572](https://github.com/apache/arrow-rs/pull/572) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Fix array equal check [\#571](https://github.com/apache/arrow-rs/pull/571) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- Sort binary [\#569](https://github.com/apache/arrow-rs/pull/569) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([waynexia](https://github.com/waynexia))
- Add len\(\) to InMemoryWriteableCursor [\#564](https://github.com/apache/arrow-rs/pull/564) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([mosyp](https://github.com/mosyp))
- Doctest for StructArray. [\#562](https://github.com/apache/arrow-rs/pull/562) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- Bump prost and tonic [\#560](https://github.com/apache/arrow-rs/pull/560) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([PsiACE](https://github.com/PsiACE))
- make `has_min_max_set` as pub fn [\#559](https://github.com/apache/arrow-rs/pull/559) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([b41sh](https://github.com/b41sh))
- Update triplet.rs to support REPEATED field with null value at one place [\#556](https://github.com/apache/arrow-rs/pull/556) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([aiglematth](https://github.com/aiglematth))
- Exclude .github in rat files [\#551](https://github.com/apache/arrow-rs/pull/551) ([alamb](https://github.com/alamb))
- Fix default arrow build [\#533](https://github.com/apache/arrow-rs/pull/533) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Add tests for building applications using arrow with different feature flags [\#532](https://github.com/apache/arrow-rs/pull/532) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alamb](https://github.com/alamb))
- Remove unused futures dependency from arrow-flight [\#528](https://github.com/apache/arrow-rs/pull/528) [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- CI: update rust nightly and packed\_simd [\#525](https://github.com/apache/arrow-rs/pull/525) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Support `StringArray` creation from String Vec [\#522](https://github.com/apache/arrow-rs/pull/522) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([silathdiir](https://github.com/silathdiir))
- Fix parquet benchmark schema [\#513](https://github.com/apache/arrow-rs/pull/513) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix parquet definition levels [\#511](https://github.com/apache/arrow-rs/pull/511) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix for primitive and boolean take kernel for nullable indices with an offset [\#509](https://github.com/apache/arrow-rs/pull/509) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([jhorstmann](https://github.com/jhorstmann))
- Bump flatbuffers [\#499](https://github.com/apache/arrow-rs/pull/499) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([PsiACE](https://github.com/PsiACE))
- implement second/minute helpers for temporal [\#493](https://github.com/apache/arrow-rs/pull/493) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ovr](https://github.com/ovr))
- special case concatenating single element array shortcut [\#492](https://github.com/apache/arrow-rs/pull/492) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([Jimexist](https://github.com/Jimexist))
- update docs to reflect recent changes \(joins and window functions\) [\#489](https://github.com/apache/arrow-rs/pull/489) ([Jimexist](https://github.com/Jimexist))
- Update rand, proc-macro and zstd dependencies [\#488](https://github.com/apache/arrow-rs/pull/488) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([alamb](https://github.com/alamb))
- Doctest for GenericListArray. [\#474](https://github.com/apache/arrow-rs/pull/474) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([novemberkilo](https://github.com/novemberkilo))
- remove stale comment on `ArrayData` equality and update unit tests [\#472](https://github.com/apache/arrow-rs/pull/472) ([Jimexist](https://github.com/Jimexist))
- remove unused patch file [\#471](https://github.com/apache/arrow-rs/pull/471) ([Jimexist](https://github.com/Jimexist))
- fix clippy warnings for rust 1.53 [\#470](https://github.com/apache/arrow-rs/pull/470) ([Jimexist](https://github.com/Jimexist))
- Fix PR labeler [\#468](https://github.com/apache/arrow-rs/pull/468) ([Dandandan](https://github.com/Dandandan))
- Tweak dev backporting docs [\#466](https://github.com/apache/arrow-rs/pull/466) ([alamb](https://github.com/alamb))
- Unvendor Archery [\#459](https://github.com/apache/arrow-rs/pull/459) ([kszucs](https://github.com/kszucs))
- Add sort boolean benchmark [\#457](https://github.com/apache/arrow-rs/pull/457) ([alamb](https://github.com/alamb))
- Add C data interface for decimal128 and timestamp [\#453](https://github.com/apache/arrow-rs/pull/453) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([alippai](https://github.com/alippai))
- Implement the Iterator trait for the json Reader. [\#451](https://github.com/apache/arrow-rs/pull/451) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([LaurentMazare](https://github.com/LaurentMazare))
- Update release docs + release email template [\#450](https://github.com/apache/arrow-rs/pull/450) ([alamb](https://github.com/alamb))
- remove clippy unnecessary wraps suppresions in cast kernel [\#449](https://github.com/apache/arrow-rs/pull/449) ([Jimexist](https://github.com/Jimexist))
- Use partition for bool sort [\#448](https://github.com/apache/arrow-rs/pull/448) ([Jimexist](https://github.com/Jimexist))
- remove unnecessary wraps in sort [\#445](https://github.com/apache/arrow-rs/pull/445) ([Jimexist](https://github.com/Jimexist))
- Python FFI bridge for Schema, Field and DataType  [\#439](https://github.com/apache/arrow-rs/pull/439) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([kszucs](https://github.com/kszucs))
- Update release Readme.md [\#436](https://github.com/apache/arrow-rs/pull/436) ([alamb](https://github.com/alamb))
- Derive Eq and PartialEq for SortOptions [\#425](https://github.com/apache/arrow-rs/pull/425) ([tustvold](https://github.com/tustvold))
- refactor lexico sort for future code reuse [\#423](https://github.com/apache/arrow-rs/pull/423) ([Jimexist](https://github.com/Jimexist))
- Reenable MIRI check on PRs [\#421](https://github.com/apache/arrow-rs/pull/421) ([alamb](https://github.com/alamb))
- Sort by float lists [\#420](https://github.com/apache/arrow-rs/pull/420) ([medwards](https://github.com/medwards))
- Fix out of bounds read in bit chunk iterator [\#416](https://github.com/apache/arrow-rs/pull/416) ([jhorstmann](https://github.com/jhorstmann))
- Doctests for DecimalArray. [\#414](https://github.com/apache/arrow-rs/pull/414) ([novemberkilo](https://github.com/novemberkilo))
- Add Decimal to CsvWriter and improve debug display [\#406](https://github.com/apache/arrow-rs/pull/406) ([alippai](https://github.com/alippai))
- MINOR: update install instruction [\#400](https://github.com/apache/arrow-rs/pull/400) ([alippai](https://github.com/alippai))
- use prettier to auto format md files [\#398](https://github.com/apache/arrow-rs/pull/398) ([Jimexist](https://github.com/Jimexist))
- make slice work for nested types [\#389](https://github.com/apache/arrow-rs/pull/389) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([nevi-me](https://github.com/nevi-me))
- window::shift to work for all array types [\#388](https://github.com/apache/arrow-rs/pull/388) ([Jimexist](https://github.com/Jimexist))
- add more tests for window::shift and handle boundary cases [\#386](https://github.com/apache/arrow-rs/pull/386) ([Jimexist](https://github.com/Jimexist))
- Implement faster arrow array reader [\#384](https://github.com/apache/arrow-rs/pull/384) ([yordan-pavlov](https://github.com/yordan-pavlov))
- Add set\_bit to BooleanBufferBuilder to allow mutating bit in index [\#383](https://github.com/apache/arrow-rs/pull/383) ([boazberman](https://github.com/boazberman))
- make sure that only concat preallocates buffers [\#382](https://github.com/apache/arrow-rs/pull/382) ([ritchie46](https://github.com/ritchie46))
- Respect max rowgroup size in Arrow writer [\#381](https://github.com/apache/arrow-rs/pull/381) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix typo in release script, update release location [\#380](https://github.com/apache/arrow-rs/pull/380) ([alamb](https://github.com/alamb))
- Doctests for FixedSizeBinaryArray [\#378](https://github.com/apache/arrow-rs/pull/378) ([novemberkilo](https://github.com/novemberkilo))
- Simplify shift kernel using new\_null\_array [\#370](https://github.com/apache/arrow-rs/pull/370) ([Dandandan](https://github.com/Dandandan))
- allow `SliceableCursor` to be constructed from an `Arc` directly [\#369](https://github.com/apache/arrow-rs/pull/369) ([crepererum](https://github.com/crepererum))
- Add doctest for ArrayBuilder [\#367](https://github.com/apache/arrow-rs/pull/367) ([alippai](https://github.com/alippai))
- Fix version in readme [\#365](https://github.com/apache/arrow-rs/pull/365) ([domoritz](https://github.com/domoritz))
- Remove superfluous space [\#363](https://github.com/apache/arrow-rs/pull/363) ([domoritz](https://github.com/domoritz))
- Add crate badges [\#362](https://github.com/apache/arrow-rs/pull/362) ([domoritz](https://github.com/domoritz))
- Disable MIRI check until it runs cleanly on CI [\#360](https://github.com/apache/arrow-rs/pull/360) ([alamb](https://github.com/alamb))
- Only register Flight.proto with cargo if it exists [\#351](https://github.com/apache/arrow-rs/pull/351) ([tustvold](https://github.com/tustvold))
- Reduce memory usage of concat \(large\)utf8 [\#348](https://github.com/apache/arrow-rs/pull/348) ([ritchie46](https://github.com/ritchie46))
- Fix filter UB and add fast path [\#341](https://github.com/apache/arrow-rs/pull/341) ([ritchie46](https://github.com/ritchie46))
- Automatic cherry-pick script [\#339](https://github.com/apache/arrow-rs/pull/339) ([alamb](https://github.com/alamb))
- Doctests for BooleanArray. [\#338](https://github.com/apache/arrow-rs/pull/338) ([novemberkilo](https://github.com/novemberkilo))
- feature gate ipc reader/writer [\#336](https://github.com/apache/arrow-rs/pull/336) ([ritchie46](https://github.com/ritchie46))
- Add ported Rust release verification script [\#331](https://github.com/apache/arrow-rs/pull/331) ([wesm](https://github.com/wesm))
- Doctests for StringArray and LargeStringArray. [\#330](https://github.com/apache/arrow-rs/pull/330) ([novemberkilo](https://github.com/novemberkilo))
- inline PrimitiveArray::value [\#329](https://github.com/apache/arrow-rs/pull/329) ([ritchie46](https://github.com/ritchie46))
- Enable wasm32 as a target architecture for the SIMD feature  [\#324](https://github.com/apache/arrow-rs/pull/324) ([roee88](https://github.com/roee88))
- Fix undefined behavior in FFI and enable MIRI checks on CI [\#323](https://github.com/apache/arrow-rs/pull/323) ([roee88](https://github.com/roee88))
- Mutablebuffer::shrink\_to\_fit [\#318](https://github.com/apache/arrow-rs/pull/318) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- Add \(simd\) modulus op [\#317](https://github.com/apache/arrow-rs/pull/317) ([gangliao](https://github.com/gangliao))
- feature gate csv functionality [\#312](https://github.com/apache/arrow-rs/pull/312) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([ritchie46](https://github.com/ritchie46))
- \[Minor\] Version upgrades [\#304](https://github.com/apache/arrow-rs/pull/304) ([Dandandan](https://github.com/Dandandan))
- Remove old release scripts [\#293](https://github.com/apache/arrow-rs/pull/293) ([alamb](https://github.com/alamb))
- Add Send to the ArrayBuilder trait [\#291](https://github.com/apache/arrow-rs/pull/291) ([Max-Meldrum](https://github.com/Max-Meldrum))
- Added changelog generator script and configuration. [\#289](https://github.com/apache/arrow-rs/pull/289) ([jorgecarleitao](https://github.com/jorgecarleitao))
- manually bump development version [\#288](https://github.com/apache/arrow-rs/pull/288) ([nevi-me](https://github.com/nevi-me))
- Fix FFI and add support for Struct type [\#287](https://github.com/apache/arrow-rs/pull/287) ([roee88](https://github.com/roee88))
- Fix subtraction underflow when sorting string arrays with many nulls [\#285](https://github.com/apache/arrow-rs/pull/285) ([medwards](https://github.com/medwards))
- Speed up bound checking in `take` [\#281](https://github.com/apache/arrow-rs/pull/281) ([Dandandan](https://github.com/Dandandan))
- Update PR template by commenting out instructions [\#278](https://github.com/apache/arrow-rs/pull/278) ([nevi-me](https://github.com/nevi-me))
- Added Decimal support to pretty-print display utility \(\#230\) [\#273](https://github.com/apache/arrow-rs/pull/273) ([mgill25](https://github.com/mgill25))
- Fix null struct and list roundtrip [\#270](https://github.com/apache/arrow-rs/pull/270) ([nevi-me](https://github.com/nevi-me))
- 1.52 clippy fixes [\#267](https://github.com/apache/arrow-rs/pull/267) ([nevi-me](https://github.com/nevi-me))
- Fix typo in csv/reader.rs [\#265](https://github.com/apache/arrow-rs/pull/265) ([domoritz](https://github.com/domoritz))
- Fix empty Schema::metadata deserialization error [\#260](https://github.com/apache/arrow-rs/pull/260) ([hulunbier](https://github.com/hulunbier))
- update datafusion and ballista doc links [\#259](https://github.com/apache/arrow-rs/pull/259) ([Jimexist](https://github.com/Jimexist))
- support full u32 and u64 roundtrip through parquet [\#258](https://github.com/apache/arrow-rs/pull/258) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([crepererum](https://github.com/crepererum))
- \[MINOR\] Added env to run rust in integration. [\#253](https://github.com/apache/arrow-rs/pull/253) ([jorgecarleitao](https://github.com/jorgecarleitao))
- \[Minor\] Made integration tests always run. [\#248](https://github.com/apache/arrow-rs/pull/248) ([jorgecarleitao](https://github.com/jorgecarleitao))
- fix parquet max\_definition for non-null structs [\#246](https://github.com/apache/arrow-rs/pull/246) ([nevi-me](https://github.com/nevi-me))
- Disabled rebase needed until demonstrate working. [\#243](https://github.com/apache/arrow-rs/pull/243) ([jorgecarleitao](https://github.com/jorgecarleitao))
- pin flatbuffers to 0.8.4 [\#239](https://github.com/apache/arrow-rs/pull/239) ([ritchie46](https://github.com/ritchie46))
- sort\_primitive result is capped to the min of limit or values.len [\#236](https://github.com/apache/arrow-rs/pull/236) ([medwards](https://github.com/medwards))
- Read list field correctly [\#234](https://github.com/apache/arrow-rs/pull/234) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] ([nevi-me](https://github.com/nevi-me))
- Fix code examples for RecordBatch::try\_from\_iter [\#231](https://github.com/apache/arrow-rs/pull/231) ([alamb](https://github.com/alamb))
- Support string dictionaries in csv reader \(\#228\) [\#229](https://github.com/apache/arrow-rs/pull/229) ([tustvold](https://github.com/tustvold))
- support LargeUtf8 in sort kernel [\#26](https://github.com/apache/arrow-rs/pull/26) ([ritchie46](https://github.com/ritchie46))
- Removed unused files [\#22](https://github.com/apache/arrow-rs/pull/22) ([jorgecarleitao](https://github.com/jorgecarleitao))
- ARROW-12504: Buffer::from\_slice\_ref set correct capacity [\#18](https://github.com/apache/arrow-rs/pull/18) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Add GitHub templates [\#17](https://github.com/apache/arrow-rs/pull/17) ([andygrove](https://github.com/andygrove))
- ARROW-12493: Add support for writing dictionary arrays to CSV and JSON [\#16](https://github.com/apache/arrow-rs/pull/16) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- ARROW-12426: \[Rust\] Fix concatentation of arrow dictionaries [\#15](https://github.com/apache/arrow-rs/pull/15) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] ([tustvold](https://github.com/tustvold))
- Update repository and homepage urls [\#14](https://github.com/apache/arrow-rs/pull/14) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)] [[arrow](https://github.com/apache/arrow-rs/labels/arrow)] [[arrow-flight](https://github.com/apache/arrow-rs/labels/arrow-flight)] ([Dandandan](https://github.com/Dandandan))
- Added rebase-needed bot [\#13](https://github.com/apache/arrow-rs/pull/13) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Added Integration tests against arrow [\#10](https://github.com/apache/arrow-rs/pull/10) ([jorgecarleitao](https://github.com/jorgecarleitao))

## [5.5.0](https://github.com/apache/arrow-rs/tree/5.5.0) (2021-09-24)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.4.0...5.5.0)

**Implemented enhancements:**

- parquet should depend on a small set of arrow features [\#800](https://github.com/apache/arrow-rs/issues/800)
- Support equality on RecordBatch [\#735](https://github.com/apache/arrow-rs/issues/735)

**Fixed bugs:**

- Converting from string to timestamp uses microseconds instead of milliseconds [\#780](https://github.com/apache/arrow-rs/issues/780)
- Document has no link to `RowColumIter` [\#762](https://github.com/apache/arrow-rs/issues/762)
- length on slices with null doesn't work [\#744](https://github.com/apache/arrow-rs/issues/744)

## [5.4.0](https://github.com/apache/arrow-rs/tree/5.4.0) (2021-09-10)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.3.0...5.4.0)

**Implemented enhancements:**

- Upgrade lexical-core to 0.8 [\#747](https://github.com/apache/arrow-rs/issues/747)
- `append_nulls` and `append_trusted_len_iter` for PrimitiveBuilder [\#725](https://github.com/apache/arrow-rs/issues/725)
- Optimize MutableArrayData::extend for null buffers [\#397](https://github.com/apache/arrow-rs/issues/397)

**Fixed bugs:**

- Arithmetic with scalars doesn't work on slices [\#742](https://github.com/apache/arrow-rs/issues/742)
- Comparisons with scalar don't work on slices [\#740](https://github.com/apache/arrow-rs/issues/740)
- `unary` kernel doesn't respect offset [\#738](https://github.com/apache/arrow-rs/issues/738)
- `new_null_array` creates invalid struct arrays [\#734](https://github.com/apache/arrow-rs/issues/734)
- --no-default-features is broken for parquet [\#733](https://github.com/apache/arrow-rs/issues/733) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- `Bitmap::len` returns the number of bytes, not bits. [\#730](https://github.com/apache/arrow-rs/issues/730)
- Decimal logical type is formatted incorrectly by print\_schema [\#713](https://github.com/apache/arrow-rs/issues/713)
- parquet\_derive does not support chrono time values [\#711](https://github.com/apache/arrow-rs/issues/711)
- Numeric overflow when formatting Decimal type [\#710](https://github.com/apache/arrow-rs/issues/710)
- The integration tests are not running [\#690](https://github.com/apache/arrow-rs/issues/690)

**Closed issues:**

- Question: Is there no way to create a DictionaryArray with a pre-arranged mapping? [\#729](https://github.com/apache/arrow-rs/issues/729)

## [5.3.0](https://github.com/apache/arrow-rs/tree/5.3.0) (2021-08-26)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.2.0...5.3.0)

**Implemented enhancements:**

- Add optimized filter kernel for regular expression matching [\#697](https://github.com/apache/arrow-rs/issues/697)
- Can't cast from timestamp array to string array [\#587](https://github.com/apache/arrow-rs/issues/587)

**Fixed bugs:**

- 'Encoding DELTA\_BYTE\_ARRAY is not supported' with parquet arrow readers [\#708](https://github.com/apache/arrow-rs/issues/708)
- Support reading json string into binary data type. [\#701](https://github.com/apache/arrow-rs/issues/701)

**Closed issues:**

- Resolve Issues with `prettytable-rs` dependency [\#69](https://github.com/apache/arrow-rs/issues/69) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]

## [5.2.0](https://github.com/apache/arrow-rs/tree/5.2.0) (2021-08-12)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.1.0...5.2.0)

**Implemented enhancements:**

- Make rand an optional dependency [\#671](https://github.com/apache/arrow-rs/issues/671)
- Remove undefined behavior in `value` method of boolean and primitive arrays [\#645](https://github.com/apache/arrow-rs/issues/645)
- Avoid materialization of indices in filter\_record\_batch for single arrays [\#636](https://github.com/apache/arrow-rs/issues/636)
- Add a note about arrow crate security / safety [\#627](https://github.com/apache/arrow-rs/issues/627)
- Allow the creation of String arrays from an interator of &Option\<&str\> [\#598](https://github.com/apache/arrow-rs/issues/598)
- Support arrow map datatype [\#395](https://github.com/apache/arrow-rs/issues/395)

**Fixed bugs:**

- Parquet fixed length byte array columns write byte array statistics [\#660](https://github.com/apache/arrow-rs/issues/660) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Parquet boolean columns write Int32 statistics [\#659](https://github.com/apache/arrow-rs/issues/659) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]
- Writing Parquet with a boolean column fails [\#657](https://github.com/apache/arrow-rs/issues/657)
- JSON decoder data corruption for large i64/u64 [\#653](https://github.com/apache/arrow-rs/issues/653)
- Incorrect min/max statistics for strings in parquet files [\#641](https://github.com/apache/arrow-rs/issues/641) [[parquet](https://github.com/apache/arrow-rs/labels/parquet)]

**Closed issues:**

- Release candidate verifying script seems work on macOS [\#640](https://github.com/apache/arrow-rs/issues/640)
- Update CONTRIBUTING  [\#342](https://github.com/apache/arrow-rs/issues/342)

## [5.1.0](https://github.com/apache/arrow-rs/tree/5.1.0) (2021-07-29)

[Full Changelog](https://github.com/apache/arrow-rs/compare/5.0.0...5.1.0)

**Implemented enhancements:**

- Make FFI\_ArrowArray empty\(\) public [\#602](https://github.com/apache/arrow-rs/issues/602)
- exponential sort can be used to speed up lexico partition kernel [\#586](https://github.com/apache/arrow-rs/issues/586)
- Implement sort\(\) for binary array [\#568](https://github.com/apache/arrow-rs/issues/568)
- primitive sorting can be improved and more consistent with and without `limit` if sorted unstably [\#553](https://github.com/apache/arrow-rs/issues/553)

**Fixed bugs:**

- Confusing memory usage with CSV reader [\#623](https://github.com/apache/arrow-rs/issues/623)
- FFI implementation deviates from specification for array release  [\#595](https://github.com/apache/arrow-rs/issues/595)
- Parquet file content is different if `~/.cargo` is in a git checkout [\#589](https://github.com/apache/arrow-rs/issues/589)
- Ensure output of MIRI is checked for success [\#581](https://github.com/apache/arrow-rs/issues/581)
- MIRI failure in `array::ffi::tests::test_struct` and other ffi tests [\#580](https://github.com/apache/arrow-rs/issues/580)
- ListArray equality check may return wrong result [\#570](https://github.com/apache/arrow-rs/issues/570)
- cargo audit failed [\#561](https://github.com/apache/arrow-rs/issues/561)
- ArrayData::slice\(\) does not work for nested types such as StructArray [\#554](https://github.com/apache/arrow-rs/issues/554)

**Documentation updates:**

- More examples of how to construct Arrays [\#301](https://github.com/apache/arrow-rs/issues/301)

**Closed issues:**

- Implement StringBuilder::append\_option [\#263](https://github.com/apache/arrow-rs/issues/263) [[arrow](https://github.com/apache/arrow-rs/labels/arrow)]



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
