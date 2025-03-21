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

# Parquet + PyArrow integration testing

This is a Rust crate that tests compatibility between Rust's Parquet implementation and PyArrow.

## How to develop

```bash
# prepare development environment (used to build wheel / install in development)
python -m venv venv
venv/bin/pip install -r requirements.txt
```

Whenever rust code changes (your changes or via git pull):

```bash
source venv/bin/activate
maturin develop
pytest -v .
```
