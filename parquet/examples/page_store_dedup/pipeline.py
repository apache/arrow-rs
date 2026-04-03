#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Full pipeline for the Parquet Page Store deduplication demo.

Steps:
  1. Prepare  – download dataset and produce 4 Parquet variant files.
  2. Build    – compile the parquet-page-store CLI binary.
  3. Ingest   – write all variants into a shared content-addressed page store.
  4. Stats    – compute and display deduplication statistics.

Usage:
    python pipeline.py [--file PATH] [--skip-prepare] [--skip-build] [--skip-ingest]

Options:
    --file PATH      Use a local Parquet file instead of downloading from HuggingFace
    --skip-prepare   Skip data preparation (variants must already exist in data/)
    --skip-build     Skip cargo build (binary must already exist)
    --skip-ingest    Skip page store ingest (pages must already exist in pages/)
"""

import argparse
import os
import shutil
import subprocess
import sys

# Ensure imports from the same directory work regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
META_DIR = os.path.join(SCRIPT_DIR, "meta")
PAGES_DIR = os.path.join(SCRIPT_DIR, "pages")
CACHE_DIR = os.path.join(SCRIPT_DIR, ".cache")

# Repo root is 3 levels up: page_store_dedup/ -> examples/ -> parquet/ -> arrow-rs/
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
BINARY = os.path.join(REPO_ROOT, "target", "release", "parquet-page-store")

HF_REPO_ID = "kszucs/pq"
HF_FILENAME = "hermes-2.5-cdc-short.parquet"

# Number of rows to reserve for the appended variant
APPEND_ROWS = 5_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fmt(n: int) -> str:
    return f"{n:,}"


def _mb(path: str) -> str:
    return f"{os.path.getsize(path) / 1e6:.1f} MB"


def _dir_size(directory: str, ext: str) -> int:
    total = 0
    for entry in os.scandir(directory):
        if entry.is_file() and entry.name.endswith(ext):
            total += entry.stat().st_size
    return total


# ---------------------------------------------------------------------------
# Step 1 – Prepare
# ---------------------------------------------------------------------------


def load_raw(path: str | None) -> tuple[pa.Table, pa.Table]:
    if path is None:
        try:
            from huggingface_hub import hf_hub_download
        except ImportError:
            sys.exit("ERROR: huggingface_hub is required. Install with: pip install huggingface_hub")

        print(f"  Downloading {HF_FILENAME} from HuggingFace ... ", end="", flush=True)
        path = hf_hub_download(
            repo_id=HF_REPO_ID,
            filename=HF_FILENAME,
            repo_type="dataset",
            cache_dir=CACHE_DIR,
        )
        print(f"done ({_mb(path)})")

    print(f"  Reading {os.path.basename(path)} ...")
    full = pq.read_table(path)
    print(f"  Full table: {_fmt(len(full))} rows, {len(full.schema)} columns")

    if len(full) <= APPEND_ROWS:
        sys.exit(f"ERROR: dataset has only {_fmt(len(full))} rows, need more than {_fmt(APPEND_ROWS)}")

    base = full.slice(0, len(full) - APPEND_ROWS)
    extra = full.slice(len(full) - APPEND_ROWS, APPEND_ROWS)
    return base, extra


def step_prepare(file_path: str | None) -> None:
    print("=" * 60)
    print("  Step 1 – Prepare dataset variants")
    print("=" * 60)

    os.makedirs(DATA_DIR, exist_ok=True)
    base, extra = load_raw(file_path)
    print()

    total_bytes = 0
    print(f"  Writing 4 variants to {DATA_DIR}/")
    print()

    out = os.path.join(DATA_DIR, "original.parquet")
    pq.write_table(base, out)
    sz = os.path.getsize(out)
    total_bytes += sz
    print(f"  original.parquet   {_fmt(len(base)):>9} rows   {sz / 1e6:>6.1f} MB  (baseline)")

    out = os.path.join(DATA_DIR, "filtered.parquet")
    mask = pc.less(pc.list_value_length(base["conversations"]), 3)
    filtered = base.filter(mask)
    pq.write_table(filtered, out)
    sz = os.path.getsize(out)
    total_bytes += sz
    pct = len(filtered) * 100 // len(base)
    print(f"  filtered.parquet   {_fmt(len(filtered)):>9} rows   {sz / 1e6:>6.1f} MB  ({pct}% of original rows kept)")

    out = os.path.join(DATA_DIR, "augmented.parquet")
    num_turns = pc.list_value_length(base["conversations"]).cast(pa.int32())
    augmented = base.append_column(pa.field("num_turns", pa.int32()), num_turns)
    pq.write_table(augmented, out)
    sz = os.path.getsize(out)
    total_bytes += sz
    print(f"  augmented.parquet  {_fmt(len(augmented)):>9} rows   {sz / 1e6:>6.1f} MB  (same rows, +1 column)")

    out = os.path.join(DATA_DIR, "appended.parquet")
    appended = pa.concat_tables([base, extra])
    pq.write_table(appended, out)
    sz = os.path.getsize(out)
    total_bytes += sz
    print(f"  appended.parquet   {_fmt(len(appended)):>9} rows   {sz / 1e6:>6.1f} MB  (+{_fmt(APPEND_ROWS)} rows appended)")

    print()
    print(f"  Total (4 independent files): {total_bytes / 1e6:.1f} MB")
    print()


# ---------------------------------------------------------------------------
# Step 2 – Build
# ---------------------------------------------------------------------------


def step_build() -> None:
    print("=" * 60)
    print("  Step 2 – Build parquet-page-store binary")
    print("=" * 60)
    print()

    cmd = ["cargo", "build", "--release", "-p", "parquet", "--features", "page_store,cli"]
    print(f"  Running: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        sys.exit(f"ERROR: cargo build failed (exit code {result.returncode})")

    print()
    print(f"  Binary: {BINARY}")
    print()


# ---------------------------------------------------------------------------
# Step 3 – Ingest into page store
# ---------------------------------------------------------------------------


def step_ingest() -> None:
    print("=" * 60)
    print("  Step 3 – Ingest Parquet files into page store")
    print("=" * 60)
    print()

    if not os.path.isfile(BINARY):
        sys.exit(f"ERROR: binary not found at {BINARY}\n       Run without --skip-build first.")

    for d in (PAGES_DIR, META_DIR):
        if os.path.isdir(d):
            shutil.rmtree(d)
        os.makedirs(d)

    inputs = sorted(
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.endswith(".parquet") and os.path.isfile(os.path.join(DATA_DIR, f))
    )
    if not inputs:
        sys.exit(f"ERROR: no .parquet files found in {DATA_DIR}")

    cmd = [BINARY, "write"] + inputs + ["--store", PAGES_DIR, "--output", META_DIR, "--compression", "snappy"]
    print(f"  Running: parquet-page-store write <{len(inputs)} files> --store pages --output meta --compression snappy")
    print()

    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    if result.returncode != 0:
        sys.exit(f"ERROR: parquet-page-store write failed (exit code {result.returncode})")

    print()


# ---------------------------------------------------------------------------
# Step 4 – Statistics
# ---------------------------------------------------------------------------


def step_stats() -> tuple[float, float]:
    print("=" * 60)
    print("  Step 4 – Deduplication statistics")
    print("=" * 60)
    print()

    # Input file sizes (top-level .parquet files only)
    input_files = sorted(
        os.path.join(DATA_DIR, f)
        for f in os.listdir(DATA_DIR)
        if f.endswith(".parquet") and os.path.isfile(os.path.join(DATA_DIR, f))
    )
    if not input_files:
        print("  No input files found — run without --skip-ingest first.")
        return 0.0, 0.0

    total_input = sum(os.path.getsize(p) for p in input_files)

    print("  Input files:")
    for path in input_files:
        sz = os.path.getsize(path)
        print(f"    {os.path.basename(path):<25} {sz / 1e6:>7.1f} MB")
    print(f"    {'Total':<25} {total_input / 1e6:>7.1f} MB")
    print()

    # Page store size
    if not os.path.isdir(PAGES_DIR):
        print("  Page store directory not found — run without --skip-ingest first.")
        return 0.0, 0.0

    page_files = [e for e in os.scandir(PAGES_DIR) if e.is_file() and e.name.endswith(".page")]
    if not page_files:
        print("  No .page files found in pages/ — run without --skip-ingest first.")
        return 0.0, 0.0

    total_pages = _dir_size(PAGES_DIR, ".page")
    page_count = len(page_files)

    ratio = total_pages / total_input
    savings = 1.0 - ratio
    bar_len = 20
    bar = "█" * round(ratio * bar_len)

    print("  Page store:")
    print(f"    Unique pages:              {page_count:>7,}")
    print(f"    Page store size:           {total_pages / 1e6:>7.1f} MB")
    print(f"    Total input size:          {total_input / 1e6:>7.1f} MB")
    print(f"    Dedup ratio (store/input): {ratio * 100:>6.1f}%  {bar}")
    print(f"    Space savings:             {savings * 100:>6.1f}%")
    print()
    print("  Note: these numbers reflect page-level deduplication within the")
    print("  page store. Block-level tools (e.g. 'de stats') operate at a")
    print("  different granularity and will report lower dedup ratios.")
    print()

    return total_input / 1e6, total_pages / 1e6


# ---------------------------------------------------------------------------
# Step 5 – Regenerate concept diagram
# ---------------------------------------------------------------------------


def step_concept(total_mb: float, store_mb: float) -> None:
    print("=" * 60)
    print("  Step 5 – Regenerate concept diagram")
    print("=" * 60)
    print()

    try:
        from concept import generate
    except ImportError:
        print("  SKIP: drawsvg not installed (pip install drawsvg)")
        print()
        return

    generate(total_mb=total_mb, store_mb=store_mb)
    print()


# ---------------------------------------------------------------------------
# Step 6 – Roundtrip verification
# ---------------------------------------------------------------------------


def step_verify() -> None:
    print("=" * 60)
    print("  Step 6 – Roundtrip verification")
    print("=" * 60)
    print()

    verify_dir = os.path.join(SCRIPT_DIR, "verify")
    if os.path.isdir(verify_dir):
        shutil.rmtree(verify_dir)
    os.makedirs(verify_dir)

    if not os.path.isfile(BINARY):
        sys.exit(f"ERROR: binary not found at {BINARY}\n       Run without --skip-build first.")

    meta_files = sorted(
        os.path.join(META_DIR, f)
        for f in os.listdir(META_DIR)
        if f.endswith(".meta.parquet") and os.path.isfile(os.path.join(META_DIR, f))
    )
    if not meta_files:
        sys.exit(f"ERROR: no .meta.parquet files found in {META_DIR}")

    all_ok = True
    for meta_path in meta_files:
        stem = os.path.basename(meta_path).replace(".meta.parquet", "")
        original_path = os.path.join(DATA_DIR, f"{stem}.parquet")
        reconstructed_path = os.path.join(verify_dir, f"{stem}.parquet")

        if not os.path.isfile(original_path):
            print(f"  SKIP {stem}: original not found in data/")
            continue

        cmd = [BINARY, "reconstruct", meta_path, "--store", PAGES_DIR, "--output", reconstructed_path]
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode != 0:
            print(f"  FAIL {stem}: reconstruction failed")
            print(result.stderr.decode())
            all_ok = False
            continue

        original = pq.read_table(original_path)
        reconstructed = pq.read_table(reconstructed_path)

        if original.equals(reconstructed, check_metadata=False):
            print(f"  OK   {stem}.parquet  ({len(original):,} rows, {len(original.schema)} columns)")
        else:
            print(f"  FAIL {stem}: data mismatch")
            orig_rows, rec_rows = len(original), len(reconstructed)
            if orig_rows != rec_rows:
                print(f"         row count: original={orig_rows:,}  reconstructed={rec_rows:,}")
            else:
                for col in original.schema.names:
                    if col not in reconstructed.schema.names:
                        print(f"         missing column in reconstructed: {col}")
                    elif not original[col].equals(reconstructed[col]):
                        print(f"         column mismatch: {col}")
            all_ok = False

    print()
    if all_ok:
        print("  All roundtrip checks passed.")
    else:
        sys.exit("ERROR: roundtrip verification failed")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", metavar="PATH", help="Use a local Parquet file instead of downloading")
    parser.add_argument("--skip-prepare", action="store_true", help="Skip data preparation step")
    parser.add_argument("--skip-build", action="store_true", help="Skip cargo build step")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip page store ingest step")
    parser.add_argument("--skip-concept", action="store_true", help="Skip concept diagram regeneration")
    parser.add_argument("--skip-verify", action="store_true", help="Skip roundtrip verification step")
    args = parser.parse_args()

    if not args.skip_prepare:
        step_prepare(args.file)

    if not args.skip_build:
        step_build()

    if not args.skip_ingest:
        step_ingest()

    total_mb, store_mb = step_stats()

    if not args.skip_concept:
        step_concept(total_mb, store_mb)

    if not args.skip_verify:
        step_verify()


if __name__ == "__main__":
    main()
