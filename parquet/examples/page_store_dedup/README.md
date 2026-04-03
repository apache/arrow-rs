# Parquet Page Store — Deduplication Demo

> **Prototype**: This is an experimental feature exploring content-defined
> chunking for Parquet.  APIs and file formats may change.

Demonstrates how Content-Defined Chunking (CDC) enables efficient deduplication
across multiple versions of a dataset using the Parquet page store writer in
Apache Arrow Rust.  The deduplication is self-contained in the Parquet writer —
no special storage system is required.

## What this demo shows

Four common dataset operations are applied to a real-world dataset
([OpenHermes-2.5](https://huggingface.co/datasets/teknium/OpenHermes-2.5)
conversational data, ~800 MB per file).  Each operation produces a separate
Parquet file.  Without a page store, storing all four files costs the full sum
of their sizes.  With the CDC page store, identical pages are stored **exactly
once** — indexed by their BLAKE3 hash — so the four files share most of their
bytes.  The resulting files can be stored anywhere.

| File | Operation |
|------|-----------|
| `original.parquet` | Baseline dataset (~996k rows) |
| `filtered.parquet` | Keep rows where `num_turns ≤ 3` |
| `augmented.parquet` | Original + computed column `num_turns` |
| `appended.parquet` | Original + 5 000 new rows appended |

## Prerequisites

```bash
pip install pyarrow matplotlib huggingface_hub
cargo build --release -p parquet --features page_store,cli
```

## Running the demo

```bash
cd parquet/examples/page_store_dedup

# Run the full pipeline: prepare data, build binary, ingest into page store, show stats
python pipeline.py

# Then generate diagrams
python diagram.py
```

Individual steps can be skipped if they've already run:

```bash
python pipeline.py --skip-prepare --skip-build   # re-run ingest + stats only
python pipeline.py --skip-prepare --skip-build --skip-ingest  # stats only
```

Outputs:
- `page_store_concept.png` — architectural overview of how shared pages work
- `page_store_savings.png` — side-by-side storage comparison with real numbers

## Using your own dataset

```bash
python pipeline.py --file /path/to/your.parquet
```

The script requires a `conversations` list column for the filtered and augmented
variants.  Adapt `pipeline.py` to your own schema as needed.

## Results

Dataset: **OpenHermes-2.5** (short conversations, `num_turns < 10`)

### Dataset variants

| File | Operation | Rows | Size |
|------|-----------|------|------|
| `original.parquet` | Baseline | 996,009 | 782.1 MB |
| `filtered.parquet` | Keep `num_turns ≤ 3` (removes 0.2% of rows) | 993,862 | 776.8 MB |
| `augmented.parquet` | Add column `num_turns` | 996,009 | 782.2 MB |
| `appended.parquet` | Append 5,000 rows | 1,001,009 | 788.6 MB |
| **Total** | | | **3,129.7 MB** |

### Page store results

| Metric | Value |
|--------|-------|
| Unique pages stored | 3,400 |
| Total page references | 15,179 |
| Page store size | 559.0 MB |
| Metadata files size | 4.4 MB |
| **Page store + metadata** | **563.4 MB** |
| **Storage saved** | **2,566.3 MB (82%)** |
| **Deduplication ratio** | **5.6×** |

### Per-file page breakdown

| File | Page refs | Unique hashes | New pages | Reused pages |
|------|-----------|---------------|-----------|--------------|
| `original.parquet` | 3,782 | 3,100 | 3,100 | 0 |
| `filtered.parquet` | 3,755 | 3,075 | 222 | 2,853 (92%) |
| `augmented.parquet` | 3,834 | 3,136 | 36 | 3,100 (98%) |
| `appended.parquet` | 3,808 | 3,125 | 42 | 3,083 (98%) |

### Key insights

1. **Adding a column** (`augmented`): only 36 new pages out of 3,136 (1.1%).
   The existing 17 columns produce identical CDC pages — only the new `num_turns`
   column contributes new pages.

2. **Appending rows** (`appended`): only 42 new pages out of 3,125 (1.3%).
   The original 996k rows' pages are unchanged; only the 5k new rows create new pages.

3. **Filtering rows** (`filtered`): 92% of pages reused despite row removal.
   Removing just 0.2% of rows barely shifts CDC boundaries — most pages are
   unchanged.  Heavier filtering (removing 20–50% of rows) would produce more new
   pages, as CDC boundaries shift further throughout the file.

4. **Net result**: 4 dataset versions stored for **563 MB instead of 3.1 GB** — an
   **82% reduction**, or equivalently, 4 versions for the cost of **0.72×** a single
   version.

## How it works

```
Standard Parquet — each file stored independently:

  original.parquet   ──►  [ page 1 ][ page 2 ][ page 3 ]...[ page N ]
  filtered.parquet   ──►  [ page 1'][ page 2 ][ page 3 ]...[ page M ]
  augmented.parquet  ──►  [ page 1 ][ page 2 ][ page 3 ]...[ page N ][ extra ]
  appended.parquet   ──►  [ page 1 ][ page 2 ][ page 3 ]...[ page N ][ new  ]

  Total: sum of all four file sizes

CDC Page Store — content-addressed, deduplicated:

  pages/
    <hash-of-page-1>.page    ←  shared by original, augmented, appended
    <hash-of-page-2>.page    ←  shared by original, filtered, augmented, appended
    <hash-of-page-3>.page    ←  shared by filtered only (boundary page)
    ...                         (only UNIQUE pages stored)

  meta/
    original.meta.parquet    ←  tiny manifest referencing page hashes
    filtered.meta.parquet
    augmented.meta.parquet
    appended.meta.parquet

  Total: ~18% of the combined file sizes
```

CDC ensures that page boundaries are **content-defined** (not fixed row
counts), so adding columns or appending rows only requires storing the small
number of new pages — the rest remain identical and are reused.

## Further reading

- [`parquet::arrow::page_store`][api] API docs
- [`parquet-page-store` CLI][cli] source

[api]: https://docs.rs/parquet/latest/parquet/arrow/page_store/index.html
[cli]: ../../src/bin/parquet-page-store.rs
