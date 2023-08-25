#!/bin/bash
#
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
#

# This script removes all but the most recent versions of arrow-rs
# from svn
#
# The older versions are in SVN history as well as available on the
# archive page https://archive.apache.org/dist/
#
# See
# https://infra.apache.org/release-download-pages.html

set -e
set -u

svn_base="https://dist.apache.org/repos/dist/release/arrow"

echo "Remove all but the most recent version"
old_releases=$(
  svn ls ${svn_base} | \
  grep -E '^arrow-object-store-rs-[0-9\.]+' | \
  sort --version-sort --reverse | \
  tail -n +2
)
for old_release_version in $old_releases; do
  echo "Remove old release ${old_release_version}"
  svn delete -m "Removing ${old_release_version}" ${svn_base}/${old_release_version}
done
