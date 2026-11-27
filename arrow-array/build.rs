// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-check-cfg=cfg(arrow_cfi)");
    // Build scripts see `cfg(sanitize)` even though it is unstable in the crate itself.
    let sanitizers = std::env::var("CARGO_CFG_SANITIZE").unwrap_or_default();
    if sanitizers.split(',').any(|s| s == "cfi" || s == "kcfi") {
        println!("cargo:rustc-cfg=arrow_cfi");
    }
}
