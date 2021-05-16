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

We would maintain two branches: `active_release` and `master`.
* All new PRs are created and merged against `master`
* All versions are created from the `active_release` branch
* Once merged to master, changes are "cherry-picked"  (via a hopefully soon to be automated process), to the `active_release` branch based on the judgement of the original PR author and maintainers.

* We do not merge breaking api changes, as defined in [Rust RFC 1105](https://github.com/rust-lang/rfcs/blob/master/text/1105-api-evolution.md) to the `active_release`

Please see the [original proposal](https://docs.google.com/document/d/1tMQ67iu8XyGGZuj--h9WQYB9inCk6c2sL_4xMTwENGc/edit?ts=60961758) document the rational of this change.

## Release Branching
We aim to release every other week from the `active_release` branch.

Every other Monday, a maintainer proposes a minor (e.g. `4.1.0` to `4.2.0`) or patch (e.g `4.1.0` to `4.1.1`) release, depending on changes to the `active_release` in the previous 2 weeks, following the process beloe.

If this release is approved by at least three PMC members, a new version from that tarball is released to crates.io later in the week.

Apache Arrow in general does synchronized major releases every three months. The Rust implementation aims to do its major releases in the same time frame.

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
[change_log.sh](https://github.com/apache/arrow-rs/blob/master/change_log.sh)

This script creates a changelog using github issues and the
labels associated with them.




# Mechanics of creating a release

## Prepare the release branch and tags

First, ensure that `active_release` contains the content of the desired release. For minor and patch releases, no additional steps are needed.

To prepare for *a major release*, change `active release` to point at the latest `master` with commands such as:

```
git checkout active_release
git fetch apache
git reset --hard apache/master
git push -f
```

### Update CHANGELOG.md + Version

Now prepare a PR to update `CHANGELOG.md` and versions on `active_release` branch to reflect the planned release.

See [#298](https://github.com/apache/arrow-rs/pull/298) for an example.

Here are the commands used to prepare the 4.1.0 release:

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
sed -i '' -e 's/5.0.0-SNAPSHOT/4.1.0/g' `find . -name 'Cargo.toml'`
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

Pick numbers in sequential order, with `0` for `rc1`, `1` for `rc1`, etc.

### Create, sign, and upload tarball

Run the  `create-tarball.sh` with the  `<version>` tag and `<rc>` and you found in previous steps:

```shell
./dev/release/create-tarball.sh 4.1.0 2
```

This script

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

There is a script in this repository which can be used to help `dev/release/verify-release-candidate.sh` assist the verification process. Run it like:

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

If the Cargo.toml in this tag already contains `version = "0.11.0"` (as it
should) then the crate can be published with the following command:

```shell
cargo publish
```

If the Cargo.toml does not have the correct version then it will be necessary
to modify it manually. Since there is now a modified file locally that is not
committed to GitHub it will be necessary to use the following command.

```shell
cargo publish --allow-dirty
```
