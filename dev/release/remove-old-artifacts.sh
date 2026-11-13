#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# This script removes all RCs of arrow-rs from svn.

set -e
set -u
set -o pipefail

echo "Remove all RCs"
dev_base_url=https://dist.apache.org/repos/dist/dev/arrow
old_rcs=$(
  svn ls ${dev_base_url}/ | \
  grep -E '^apache-arrow-rs-[0-9]' | \
  sort --version-sort
)
for old_rc in $old_rcs; do
  echo "Remove RC: ${old_rc}"
  svn \
    delete \
    -m "Remove old Apache Arrow Rust RC: ${old_rc}" \
    ${dev_base_url}/${old_rc}
done
