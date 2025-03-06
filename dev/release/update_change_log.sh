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

# invokes the changelog generator from
# https://github.com/github-changelog-generator/github-changelog-generator
#
# With the config located in
# arrow-rs/.github_changelog_generator
#
# Usage:
# ARROW_GITHUB_API_TOKEN=<TOKEN> ./update_change_log.sh

set -e

SINCE_TAG="54.2.0"
FUTURE_RELEASE="54.2.1"

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

OUTPUT_PATH="${SOURCE_TOP_DIR}/CHANGELOG.md"
OLD_OUTPUT_PATH="${SOURCE_TOP_DIR}/CHANGELOG-old.md"

# remove license header so github-changelog-generator has a clean base to append
sed -i.bak '1,21d' "${OUTPUT_PATH}"
sed -i.bak '1,21d' "${OLD_OUTPUT_PATH}"
# remove the github-changelog-generator footer from the old CHANGELOG.md
LINE_COUNT=$(wc -l <"${OUTPUT_PATH}")
sed -i.bak2 "$(( $LINE_COUNT-4+1 )),$ d" "${OUTPUT_PATH}"

# Copy the previous CHANGELOG.md to CHANGELOG-old.md
echo '<!---
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

# Historical Changelog
' | cat - "${OUTPUT_PATH}" "${OLD_OUTPUT_PATH}" > "${OLD_OUTPUT_PATH}".tmp
mv "${OLD_OUTPUT_PATH}".tmp "${OLD_OUTPUT_PATH}"

# use exclude-tags-regex to filter out tags used for object_store
# crates and only only look at tags that DO NOT begin with `object_store_`
pushd "${SOURCE_TOP_DIR}"
docker run -it --rm -e CHANGELOG_GITHUB_TOKEN="$ARROW_GITHUB_API_TOKEN" -v "$(pwd)":/usr/local/src/your-app githubchangeloggenerator/github-changelog-generator \
    --user apache \
    --project arrow-rs \
    --cache-file=.githubchangeloggenerator.cache \
    --cache-log=.githubchangeloggenerator.cache.log \
    --http-cache \
    --max-issues=300 \
    --exclude-tags-regex "^object_store_\d+\.\d+\.\d+$|-rc\d$" \
    --since-tag ${SINCE_TAG} \
    --future-release ${FUTURE_RELEASE}

sed -i.bak "s/\\\n/\n\n/" "${OUTPUT_PATH}"

# Put license header back on
echo '<!---
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
' | cat - "${OUTPUT_PATH}" > "${OUTPUT_PATH}".tmp
mv "${OUTPUT_PATH}".tmp "${OUTPUT_PATH}"
