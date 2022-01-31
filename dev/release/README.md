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

We try to release a new version of Arrow every two weeks. This cadence balances getting new features into arrow without overwhelming downstream projects with too frequent changes.

If any code has been merged to master that has a breaking API change, as defined in [Rust RFC 1105](https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md), the major version number incremented changed (e.g. `8.0.0` to `8.0.0`). Otherwise the new minor version incremented (e.g. `8.0.0` to `7.1.0`).

# Release Mechanics

## Process Overview

As part of the Apache governance model, official releases consist of
signed source tarballs approved by the PMC.

We then use the code in the approved source tarball to release to
crates.io, the Rust ecosystem's package manager.

We create a `CHANGELOG.md` so our users know what has been changed between releases.

The CHANGELOG is created automatically using
[update_change_log.sh](https://github.com/apache/arrow-rs/blob/master/dev/release/update_change_log.sh)

This script creates a changelog using github issues and the
labels associated with them.

## Prepare CHANGELOG and version:

Now prepare a PR to update `CHANGELOG.md` and versions on `master` to reflect the planned release.

See [#1141](https://github.com/apache/arrow-rs/pull/1141) for an example.

Here are the commands that could be used to prepare the 5.1.0 release:

```bash
git checkout master
git pull
git checkout -b make-release

# manully edit ./dev/release/update_change_log.sh to reflect the release version
# create the changelog
CHANGELOG_GITHUB_TOKEN=<TOKEN> ./dev/release/update_change_log.sh
# review change log / edit issues and labels if needed, rerun
git commit -a -m 'Create changelog'

# update versions
sed -i '' -e 's/5.0.0-SNAPSHOT/5.1.0/g' `find . -name 'Cargo.toml' -or -name '*.md'`
git commit -a -m 'Update version'
```

Note that when reviewing the change log, rather than editing the
`CHANGELOG.md`, it is preferred to update the issues and their labels
(e.g. add `invalid` label to exclude them from release notes)

## Prepare release candidate tarball

(Note you need to be a committer to run these scripts as they upload to the apache svn distribution servers)

### Create git tag for the release:

While the official release artifact is a signed tarball, we also tag the commit it was created for convenience and code archaeology.

Using a string such as `4.0.1` as the `<version>`, create and push the tag thusly:

```shell
git fetch apache
git tag <version> apache/master
# push tag to apache
git push apache <version>
```

### Pick an Release Candidate (RC) number

Pick numbers in sequential order, with `1` for `rc1`, `2` for `rc2`, etc.

### Create, sign, and upload tarball

Run `create-tarball.sh` with the `<version>` tag and `<rc>` and you found in previous steps:

```shell
./dev/release/create-tarball.sh 4.1.0 2
```

The `create-tarball.sh` script

1. creates and uploads a release candidate tarball to the [arrow
   dev](https://dist.apache.org/repos/dist/dev/arrow) location on the
   apache distribution svn server

2. provide you an email template to
   send to dev@arrow.apache.org for release voting.

### Vote on Release Candidate tarball

Send the email output from the script to dev@arrow.apache.org. The email should look like

```
To: dev@arrow.apache.org
Subject: [VOTE][RUST] Release Apache Arrow

Hi,

I would like to propose a release of Apache Arrow Rust
Implementation, version 4.1.0.

This release candidate is based on commit: a5dd428f57e62db20a945e8b1895de91405958c4 [1]

The proposed release tarball and signatures are hosted at [2].
The changelog is located at [3].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release.

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow Rust
[ ] +0
[ ] -1 Do not release this as Apache Arrow Rust  because...

[1]: https://github.com/apache/arrow-rs/tree/a5dd428f57e62db20a945e8b1895de91405958c4
[2]: https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-rs-4.1.0
[3]: https://github.com/apache/arrow-rs/blob/a5dd428f57e62db20a945e8b1895de91405958c4/CHANGELOG.md
```

For the release to become "official" it needs at least three PMC members to vote +1 on it.

## Verifying release candidates

The `dev/release/verify-release-candidate.sh` is a script in this repository that can assist in the verification process. Run it like:

```
./dev/release/verify-release-candidate.sh 4.1.0 2
```

#### If the release is not approved

If the release is not approved, fix whatever the problem is and try again with the next RC number

### If the release is approved,

Move tarball to the release location in SVN, e.g. https://dist.apache.org/repos/dist/release/arrow/arrow-4.1.0/, using the `release-tarball.sh` script:

```shell
./dev/release/release-tarball.sh 4.1.0 2
```

Congratulations! The release is now offical!

### Publish on Crates.io

Only approved releases of the tarball should be published to
crates.io, in order to conform to Apache Software Foundation
governance standards.

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

```shell
(cd arrow && cargo publish)
(cd arrow-flight && cargo publish)
(cd parquet && cargo publish)
(cd parquet_derive && cargo publish)
```
