// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Print the arrow version and exit

use arrow::ARROW_VERSION;

fn main() {
    println!("arrow version: {ARROW_VERSION}");
}
