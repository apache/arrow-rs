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

## Branching

Arrow maintains two branches: `active_release` and `master`.

- All new PRs are created and merged against `master`
- All versions are created from the `active_release` branch
- Once merged to master, changes are "cherry-picked" (via a hopefully soon to be automated process), to the `active_release` branch based on the judgement of the original PR author and maintainers.

- We do not merge breaking api changes, as defined in [Rust RFC 1105](https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md) to the `active_release` branch. Instead, they are merged to the `master` branch and included in the next major release.

Please see the [original proposal](https://docs.google.com/document/d/1tMQ67iu8XyGGZuj--h9WQYB9inCk6c2sL_4xMTwENGc/edit?ts=60961758) document the rational of this change.

## Release Branching

We aim to release every other week from the `active_release` branch.

Every other week, a maintainer proposes a minor (e.g. `4.1.0` to `4.2.0`) or patch (e.g `4.1.0` to `4.1.1`) release, depending on changes to the `active_release` in the previous 2 weeks, following the process below.

If this release is approved by at least three PMC members, that tarball is uploaded to the official apache distribution sites, a new version from that tarball is released to crates.io later in the week.

The overall Apache Arrow in general does synchronized major releases every three months. The Rust implementation aims to do its major releases in the same time frame.

# Release Mechanics

This directory contains the scripts used to manage an Apache Arrow Release.

# Process Overview

As part of the Apache governance model, official releases consist of
signed source tarballs approved by the PMC.

We then use the code in the approved source tarball to release to
crates.io, the Rust ecosystem's package manager.

## Branching

# Release Preparation

# Change Log

We create a `CHANGELOG.md` so our users know what has been changed between releases.

The CHANGELOG is created automatically using
[update_change_log.sh](https://github.com/apache/arrow-rs/blob/master/dev/release/update_change_log.sh)

This script creates a changelog using github issues and the
labels associated with them.

## CHANGELOG for maintenance releases
At the time of writing, the `update_change_log.sh` script does not work well with branches as it seems to intermix issues that were resolved in master.

To generate a bare bones CHANGELOG for maintenance releases, you can use a command similar to the following to get all changes between 5.0.0 and the active_release.

```shell
git log --pretty=oneline 5.0.0..apache/active_release
```

# Mechanics of creating a release

## Prepare the release branch and tags

First, ensure that `active_release` contains the content of the desired release. For minor and patch releases, no additional steps are needed.

To prepare for _a major release_, change `active release` to point at the latest `master` with commands such as:

```
git checkout active_release
git fetch apache
git reset --hard apache/master
git push -f
```

### Update CHANGELOG.md + Version

Now prepare a PR to update `CHANGELOG.md` and versions on `active_release` branch to reflect the planned release.

See [#298](https://github.com/apache/arrow-rs/pull/298) for an example.

Here are the commands that could be used to prepare the 5.1.0 release:

```bash
git checkout active_release
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
git tag <version> apache/active_release
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

#### Verifying Release Candidates

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

# Backporting

As of the time of writing, backporting to `active_release` done semi-manually.

_Note_: Since minor releases will be automatically picked up by other CI systems, it is CRITICAL to only cherry pick commits that are API compatible -- that is that do not require any code changes in crates that rely on arrow. API changes are released with the next major version.

Step 1: Pick the commit to cherry-pick.

Step 2: Create cherry-pick PR to active_release

Step 3a: If CI passes, merge cherry-pick PR

Step 3b: If CI doesn't pass or some other changes are needed, the PR should be reviewed / approved as normal prior to merge

For example, to backport `b2de5446cc1e45a0559fb39039d0545df1ac0d26` to active_release, you could use the following command

```shell
git clone git@github.com:apache/arrow-rs.git /tmp/arrow-rs

ARROW_GITHUB_API_TOKEN=$ARROW_GITHUB_API_TOKEN CHECKOUT_ROOT=/tmp/arrow-rs CHERRY_PICK_SHA=b2de5446cc1e45a0559fb39039d0545df1ac0d26 python3 dev/release/cherry-pick-pr.py
```

## Labels

There are two labels that help keep track of backporting:

1. [`cherry-picked`](https://github.com/apache/arrow-rs/labels/cherry-picked) for PRs that have been cherry-picked/backported to `active_release`
2. [`release-cherry-pick`](https://github.com/apache/arrow-rs/labels/release-cherry-pick) for the PRs that are the cherry pick

You can find candidates to cherry pick using [this filter](https://github.com/apache/arrow-rs/pulls?q=is%3Apr+is%3Aclosed+-label%3Arelease-cherry-pick+-label%3Acherry-picked)

## Rationale for creating PRs on cherry picked PRs:

1. PRs are a natural place to run the CI tests to make sure there are no logical conflicts
2. PRs offer a place for the original author / committers to comment and say it should/should not be backported.
3. PRs offer a way to make cleanups / fixups and approve (if needed) for non cherry pick PRs
4. There is an additional control / review when the candidate release is created
