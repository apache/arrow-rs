<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Release Process

## Overview

This file documents the release process for the "Rust Arrow Crates": `arrow`, `arrow-flight`, `parquet`, and `parquet-derive`.

### The Rust Arrow Crates

The Rust Arrow Crates are interconnected (e.g. `parquet` has an optional dependency on `arrow`) so we increment and release all of them together.

If any code has been merged to main that has a breaking API change, as defined
in [Rust RFC 1105] the major version number is incremented (e.g. `9.0.2` to `10.0.0`).
Otherwise the new minor version incremented (e.g. `9.0.2` to `9.1.0`).

[rust rfc 1105]: https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md

# Release Mechanics

## Process Overview

As part of the Apache governance model, official releases consist of
signed source tarballs approved by the Arrow PMC.

We then use the code in the approved source tarball to release to
crates.io, the Rust ecosystem's package manager.

We create a `CHANGELOG.md` so our users know what has been changed between releases.

## Prepare CHANGELOG and version

- Ensure [`git-cliff`](https://git-cliff.org/docs/installation/) is installed

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
git commit -a -m 'Update version'

# Assuming remote name is apache; if named differently ensure this is changed,
# since we need the tags from the GitHub repository
git fetch apache --tags

# ensure your github token is available
export GITHUB_TOKEN=<TOKEN>

# e.g. TAG=60.0.0; this is just used to format the CHANGELOG, so tag doesn't
# need to exist yet, and don't put an RC tag either
# (we are excluding our above version bump commit from the changelog)
git-cliff --tag <TAG> --unreleased --output CHANGELOG.md --skip-commit $(git rev-parse HEAD)

# review change log, adjust labels on PR's as required
# can rerun above git-cliff command if needed

# commit the changelog
git commit -a -m 'Create changelog'

git push
```

Note that when reviewing the change log, rather than editing the
`CHANGELOG.md`, it is preferred to update the PRs and their labels
(e.g. add `development-process` label to exclude them from release notes)

Merge this PR to `main` prior to the next step.

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

Follow [these
instructions](https://doc.rust-lang.org/cargo/reference/publishing.html) to
create an account and login to crates.io before asking to be added as an owner
of the [arrow crate](https://crates.io/crates/arrow).

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
(cd arrow-avro && cargo publish)
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
