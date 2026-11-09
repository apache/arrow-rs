// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#[cfg(feature = "encryption")]
mod encryption;
mod encryption_agnostic;
#[cfg(all(feature = "encryption", feature = "async"))]
mod encryption_async;
#[cfg(not(feature = "encryption"))]
mod encryption_disabled;
#[cfg(feature = "encryption")]
mod encryption_util;
