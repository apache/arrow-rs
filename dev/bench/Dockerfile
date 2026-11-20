# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Benchmarks a pull request against main, the same way as the benchmark bot,
# and exports the results. See README.md in this directory.

FROM rust:1-bookworm AS critcmp
# The bot uses critcmp 0.1.8, so the table has the same format.
RUN cargo install --locked --version 0.1.8 critcmp

FROM rust:1-bookworm AS bench
ARG PR
ARG BENCH
ARG BENCH_FILTER=
COPY --from=critcmp /usr/local/cargo/bin/critcmp /usr/local/bin/
# Docker fetches these again when main or the pull request has new commits.
ADD --keep-git-dir=true https://github.com/apache/arrow-rs.git#main /arrow-rs/main
ADD --keep-git-dir=true https://github.com/apache/arrow-rs.git#pull/${PR}/head /arrow-rs/pr
# Same toolchain, LTO, and features as the bot. The toolchain of the image
# replaces the one in rust-toolchain.toml.
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION} \
    CARGO_PROFILE_BENCH_LTO=thin \
    CARGO_PROFILE_RELEASE_LTO=thin \
    CRITERION_HOME=/results/criterion
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/target <<'EOF'
set -eu
# Each side needs its own target directory. Cargo gives both checkouts the
# same artifact paths, so with a shared one both sides run the same binary.
bench() {
  side=$1
  shift
  cd "/arrow-rs/$side"
  CARGO_TARGET_DIR="/target/$side" cargo bench \
    --features=arrow,async,test_common,experimental,object_store --bench "$BENCH" "$@"
}
for side in main pr; do
  cd "/arrow-rs/$side"
  # Cargo detects changes by mtime, and a cached checkout can be older than
  # the last build, so touch the sources when the commit changed.
  sha=$(git rev-parse HEAD)
  if [ "$(cat "/target/$side.sha" 2>/dev/null)" != "$sha" ]; then
    find . -name '*.rs' -exec touch {} +
    echo "$sha" >"/target/$side.sha"
  fi
  cargo update --quiet
  bench "$side" --no-run
done
bench main -- --save-baseline base ${BENCH_FILTER:+"$BENCH_FILTER"}
bench pr -- --save-baseline changed ${BENCH_FILTER:+"$BENCH_FILTER"}
cd /results
critcmp --target-dir . --export base >base.json
critcmp --target-dir . --export changed >changed.json
critcmp --target-dir . --color never base changed >critcmp.txt
{
  echo "main: $(git -C /arrow-rs/main rev-parse HEAD)"
  echo "pull request $PR: $(git -C /arrow-rs/pr rev-parse HEAD)"
  rustc -vV
  uname -a
  lscpu
} >env.txt
cat critcmp.txt
EOF

FROM scratch
COPY --from=bench /results /
