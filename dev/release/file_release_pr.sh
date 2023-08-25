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
# generates a standard release PR like this:
# https://github.com/apache/arrow-rs/pull/2734
#
# Usage:
# export BRANCH=<RELEASE_BRANCH> && export GITHUB_USERNAME=<USERNAME> && export GITHUB_TOKEN=<TOKEN> && ./file_release_pr.sh

set -e

FUTURE_RELEASE="35.0.0"
ISSUE_NUMBER=3830

TITLE="Update version to \`$FUTURE_RELEASE\` and update \`CHANGELOG\`"
BODY="# Which issue does this PR close?\n\nCloses #$ISSUE_NUMBER.\n\n# Rationale for this change\nPrepare for biweekly release\n\n# What changes are included in this PR?\n\n# Are there any user-facing changes?\nYes"
DATA="{\"title\":\"$TITLE\", \"body\":\"$BODY\", \"head\":\"$GITHUB_USERNAME:$BRANCH\",\"base\":\"master\"}"

# Create the pull request
curl -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $GITHUB_TOKEN" \
    https://api.github.com/repos/apache/arrow-rs/pulls \
    -d "$DATA"
