<!---
  SPDX-License-Identifier: Apache-2.0
  SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
  SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Apache Parquet Geometry/Geography Rust Implementation Details

[![crates.io](https://img.shields.io/crates/v/parquet-geospatial.svg)](https://crates.io/crates/parquet-geospatial)
[![docs.rs](https://img.shields.io/docsrs/parquet-geospatial.svg)](https://docs.rs/parquet/latest/parquet-geospatial/)

This crate contains implementation details for the [Geometry and Geography Encoding] from
[Apache Parquet], including utilities for calculating geospatial column statistics and writing
tests. This software is developed as part of the [Apache Arrow] project.

[Geometry and Geography Encoding]: https://github.com/apache/parquet-format/blob/master/Geospatial.md
[Apache Parquet]: https://parquet.apache.org/
[Apache Arrow]: https://arrow.apache.org/

Please see the [API documentation](https://docs.rs/parquet-geospatial/latest) for more details.

## License

Licensed under the Apache License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>.
