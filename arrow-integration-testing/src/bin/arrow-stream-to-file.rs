// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Converts an Arrow IPC stream to an Arrow IPC file.

use std::io;

use arrow::error::Result;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::FileWriter;

fn main() -> Result<()> {
    let mut arrow_stream_reader = StreamReader::try_new(io::stdin(), None)?;
    let schema = arrow_stream_reader.schema();

    let mut writer = FileWriter::try_new(io::stdout(), &schema)?;

    arrow_stream_reader.try_for_each(|batch| writer.write(&batch?))?;
    writer.finish()?;

    Ok(())
}
