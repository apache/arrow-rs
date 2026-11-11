<!---
  SPDX-License-Identifier: Apache-2.0
  SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
  SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Release Process

## Overview

This file documents the release process for the "Rust Arrow Crates": `arrow`, `arrow-flight`, `parquet`, and `parquet-derive`.

For more information on the Apache release process, see [release creation process](https://infra.apache.org/release-publishing.html)
and [signing releases](https://infra.apache.org/release-signing.html).

### The Rust Arrow Crates

The Rust Arrow Crates are interconnected (e.g. `parquet` has an optional dependency on `arrow`) so we increment and release all of them together.

If any code has been merged to main that has a breaking API change, as defined
in [Rust RFC 1105] the major version number is incremented (e.g. `9.0.2` to `10.0.0`).
Otherwise the new minor version incremented (e.g. `9.0.2` to `9.1.0`).

[rust rfc 1105]: https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md

# Release Mechanics

## Prerequisites

The following tools need to be installed locally for these instructions:

- [Subversion](https://subversion.apache.org/)
- [`git-cliff`](https://git-cliff.org/docs/installation/)
- [`hawkeye`](https://github.com/fast/hawkeye)
- If on MacOS, [`gnu-tar`](https://formulae.brew.sh/formula/gnu-tar)

Make sure `gpg` is installed and you have setup your key. See reference documentation
on this process:

- https://infra.apache.org/release-signing.html
- https://infra.apache.org/openpgp.html
- https://spark.apache.org/release-process.html

Also ensure you have [an account on crates.io](https://doc.rust-lang.org/cargo/reference/publishing.html)
and are owner of all the crates; see owners of the [arrow crate](https://crates.io/crates/arrow)
who can invite you to be an owner.

## Process Overview

As part of the Apache governance model, official releases consist of
signed source tarballs approved by the Arrow PMC.

We then use the code in the approved source tarball to release to
crates.io, the Rust ecosystem's package manager.

We create a `CHANGELOG.md` so our users know what has been changed between releases.

## Prepare CHANGELOG and version

Now prepare a PR to update `CHANGELOG.md` and versions on `main` to reflect the planned release.

First copy the contents of `CHANGELOG.md` into `CHANGELOG-old.md`.

Then do this in the root of this repository. For example [#2323](https://github.com/apache/arrow-rs/pull/2323)

```bash
git checkout main
git pull
git checkout -b <RELEASE_BRANCH>

# Update versions.
sed -i '' -e 's/14.0.0/39.0.0/g' `find . -name 'Cargo.toml' -or -name '*.md' | grep -v CHANGELOG.md | grep -v CHANGELOG-old.md | grep -v README.md`
cargo check # update Cargo.lock with new versions

# OPTIONAL: for non-major version updates, sanity check there are no semver violations
# https://github.com/obi1kenobi/cargo-semver-checks
cargo semver-checks --baseline-version <PREVIOUS_VERSION>

# Assuming remote name is apache; if named differently ensure this is changed,
# since we need the tags from the GitHub repository
git fetch apache --tags

# Used by git-cliff to retrieve PR information from GitHub API
export GITHUB_TOKEN=<TOKEN>

# e.g. TAG=60.0.0; this is just used to format the CHANGELOG, so tag doesn't
# need to exist yet, and don't put an RC tag either
# Replace 59.3.0 with previous tag (on the same branch)
git-cliff '59.3.0..' --tag <TAG> --output CHANGELOG.md

# review change log, adjust labels on PR's as required
# can rerun above git-cliff command if needed

# commit the version bump & changelog
git commit -a -m 'Bump version & create changelog'

git push
```

Note that when reviewing the change log, rather than editing the
`CHANGELOG.md`, it is preferred to update the PRs and their labels
(e.g. add `development-process` label to exclude them from release notes)

Merge this PR to `main` prior to the next step.

## Verify Miri results (required)

Before cutting a release candidate, run the
[Miri workflow](https://github.com/apache/arrow-rs/actions/workflows/miri.yaml)
on `main`:

```shell
gh workflow run miri.yaml --repo apache/arrow-rs --ref main
```

All partitions must pass before creating the RC tag. Review the results and
link the run in the release tracking issue.

## Prepare release candidate tarball

After you have merged the updates to the `CHANGELOG` and version,
create a release candidate using the following steps. Note you need to
be a committer to run these scripts as they upload to the apache `svn`
distribution servers.

### Pick a Release Candidate (RC) number

Pick numbers in sequential order, with `1` for `rc1`, `2` for `rc2`, etc.

### Create git tag for the release

While the official release artifact is a signed tarball, we also tag the commit it was created for convenience and code archaeology.

Use a string such as `43.0.0` as the `<version>`.

Create and push the tag thusly (for example, for version `4.1.0` and `rc2` would be `4.1.0-rc2`):

```shell
git fetch apache
git tag <version>-<rc> apache/main
# push tag to apache
git push apache <version>-<rc>
```

### Create, sign, and upload tarball

Run `create-tarball.sh` with the `<version>` tag and `<rc>` and you found in previous steps.

Rust Arrow Crates:

```shell
./dev/release/create-tarball.sh 4.1.0 2
```

The `create-tarball.sh` script

1. creates and uploads a release candidate tarball to the [arrow
   dev](https://dist.apache.org/repos/dist/dev/arrow) location on the
   apache distribution svn server

2. provide you an email template to
   send to <dev@arrow.apache.org> for release voting.

### Vote on Release Candidate tarball

Send an email, based on the output from the script to <dev@arrow.apache.org>.
See an [example of how the email should look](https://lists.apache.org/thread/2vpxdt6n7kzo72sxpr7q8yyby4495gnk).

For the release to become "official" it needs at least three Apache Arrow PMC members to vote +1 on it.

## Verifying release candidates

The `dev/release/verify-release-candidate.sh` script in this repository can assist in the verification process. Run it like:

```
./dev/release/verify-release-candidate.sh 4.1.0 2
```

#### If the release is not approved

If the release is not approved, fix whatever the problem is and try again with the next RC number

### If the release is approved

Then, create a new release on GitHub using the tag `<version>` (e.g. `4.1.0`).

Push the release tag to github

```shell
git tag <version> <version>-<rc>
git push apache <version>
```

Move tarball to the release location in SVN, e.g. <https://dist.apache.org/repos/dist/release/arrow/arrow-rs-4.1.0/>, using the `release-tarball.sh` script:

```shell
./dev/release/release-tarball.sh 4.1.0 2
```

Congratulations! The release is now official!

### Check the GitHub release

The [`release.yml`] workflow automatically creates a github release for the tag.
Check that the release is created and contains the correct changelog here:
<https://github.com/apache/arrow-rs/releases>

[`release.yml`]: https://github.com/apache/arrow-rs/blob/main/.github/workflows/release.yml#L1-L0

### Publish on Crates.io

It is important that only approved releases of the tarball should be published
to crates.io, in order to conform to Apache Software Foundation governance
standards.

An Arrow committer can publish this crate after an official project release has
been made to crates.io using the following instructions.

Download and unpack the official release tarball

Verify that the Cargo.toml in the tarball contains the correct version
(e.g. `version = "0.11.0"`) and then publish the crate with the
following commands

Rust Arrow Crates:

```shell
(cd arrow-buffer && cargo publish)
(cd arrow-schema && cargo publish)
(cd arrow-data && cargo publish)
(cd arrow-array && cargo publish)
(cd arrow-cmp && cargo publish)
(cd arrow-select && cargo publish)
(cd arrow-ord && cargo publish)
(cd arrow-cast && cargo publish)
(cd arrow-ipc && cargo publish)
(cd arrow-csv && cargo publish)
(cd arrow-json && cargo publish)
(cd arrow-arith && cargo publish)
(cd arrow-string && cargo publish)
(cd arrow-row && cargo publish)
(cd arrow-pyarrow && cargo publish)
(cd arrow && cargo publish)
(cd arrow-avro && cargo publish)
(cd arrow-flight && cargo publish)
(cd parquet-variant && cargo publish)
(cd parquet-variant-json && cargo publish)
(cd parquet-variant-compute && cargo publish)
(cd parquet-geospatial && cargo publish)
(cd parquet && cargo publish)
(cd parquet_derive && cargo publish)
(cd arrow-integration-test && cargo publish)
```

## Fish version

Some commands which differ in fish shell:

```fish
set -gx GITHUB_TOKEN "token"
sed -i '' -e 's/59.3.0/59.3.1/g' (find . -name 'Cargo.toml' -or -name '*.md' | grep -v CHANGELOG.md | grep -v CHANGELOG-old.md | grep -v README.md)

# manually do arrow-buffer for sanity check
cd arrow-buffer
cargo publish
cd ..
for crate in \
    arrow-schema \
    arrow-data \
    arrow-array \
    arrow-cmp \
    arrow-select \
    arrow-ord \
    arrow-cast \
    arrow-ipc \
    arrow-csv \
    arrow-json \
    arrow-arith \
    arrow-string \
    arrow-row \
    arrow-pyarrow \
    arrow \
    arrow-avro \
    arrow-flight \
    parquet-variant \
    parquet-variant-json \
    parquet-variant-compute \
    parquet-geospatial \
    parquet \
    parquet_derive \
    arrow-integration-test
    fish -c "cd $crate; and cargo publish"; or break
end

# Unset when done
set -e GITHUB_TOKEN
```
