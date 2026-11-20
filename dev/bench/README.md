<!---
  SPDX-License-Identifier: Apache-2.0
  SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
  SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Benchmark a pull request on Linux

The `Dockerfile` in this directory benchmarks a pull request against `main`,
the same way as the [benchmark bot], and exports the results to the host. It
uses the same cargo features, thin LTO, and `critcmp` version as the bot, so
the table has the same format as the bot comments.

[benchmark bot]: https://github.com/adriangb/datafusion-benchmarking

## Usage

```bash
docker build dev/bench --progress=plain \
  --build-arg PR=12342 \
  --build-arg BENCH=cast_kernels \
  --build-arg BENCH_FILTER=decimal \
  --output ~/arrow-bench-out/12342
```

- `PR`: pull request number.
- `BENCH`: criterion bench target.
- `BENCH_FILTER`: criterion filter regex. Optional.

The output directory contains:

- `critcmp.txt`: the comparison table. `base` is `main` and `changed` is the
  pull request.
- `base.json` and `changed.json`: `critcmp --export` output. Compare them
  later, or with other runs, with `critcmp base.json changed.json`.
- `criterion/`: the raw criterion data.
- `env.txt`: commits, toolchain, kernel, and CPU.

Docker caches the build. A later run fetches new commits of `main` and the pull
request and only rebuilds what changed. If nothing changed, Docker returns the
cached results. Add `--no-cache-filter bench` to measure again.
`docker builder prune` removes the cache.

## Interpreting results

- The baseline is the latest commit of `main`, while the bot uses the
  merge-base. Rebase the pull request to compare the same thing.
- On an x86-64 or arm64 Linux host, the results are comparable to the bot.
- On Apple Silicon, Docker runs a native `linux/arm64` VM. The CPU caches and
  memory bandwidth are still those of the Mac, so use it to iterate on
  benchmarks and filters, not for final numbers. `--platform linux/amd64` runs
  under emulation and is not useful for measurements.
- The bot stays the source of truth for performance results in pull requests.
