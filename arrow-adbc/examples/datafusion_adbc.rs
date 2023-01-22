// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Example of implementing ADBC on top of DataFusion.
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use arrow_adbc::error::{AdbcError, AdbcStatusCode};
use arrow_adbc::ffi::AdbcObjectDepth;
use arrow_adbc::interface::{AdbcConnection, AdbcDatabase, AdbcStatement};

enum Error {
    NotImplemented(&'static str),
    Generic(String),
}

impl AdbcError for Error {
    fn message(&self) -> &str {
        match self {
            Error::NotImplemented(msg) => msg,
            Error::Generic(msg) => &msg,
        }
    }

    fn status_code(&self) -> AdbcStatusCode {
        match self {
            Error::NotImplemented(_) => AdbcStatusCode::NotImplemented,
            Error::Generic(_) => AdbcStatusCode::Unknown,
        }
    }
}

struct Database {
    options: Mutex<HashMap<String, String>>,
}

impl Default for Database {
    fn default() -> Self {
        Database {
            options: Mutex::new(HashMap::new()),
        }
    }
}

impl AdbcDatabase for Database {
    type Error = Error;

    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error> {
        self.options
            .lock()
            .expect("Options mutex is poisoned!")
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn init(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct Connection {
    database: RefCell<Option<Arc<Database>>>,
    options: RefCell<HashMap<String, String>>,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            database: RefCell::new(None),
            options: RefCell::new(HashMap::new()),
        }
    }
}

impl AdbcConnection for Connection {
    type Error = Error;
    type DatabaseType = Database;

    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error> {
        self.options
            .borrow_mut()
            .insert(key.to_string(), value.to_string());

        Ok(())
    }

    fn init(&self, database: Arc<Self::DatabaseType>) -> Result<(), Self::Error> {
        if self.database.borrow().is_none() {
            self.database.replace(Some(database));
            Ok(())
        } else {
            Err(Error::Generic(
                "Connection has already been initialized.".to_string(),
            ))
        }
    }

    fn get_info(
        &self,
        info_codes: &[u32],
    ) -> Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error> {
        Err(Error::NotImplemented("get_info"))
    }

    fn get_objects(
        &self,
        depth: AdbcObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: &[&str],
        column_name: Option<&str>,
    ) -> Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error> {
        Err(Error::NotImplemented("get_objects"))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow::datatypes::Schema, Self::Error> {
        Err(Error::NotImplemented("get_table_schema"))
    }

    fn get_table_types(
        &self,
    ) -> Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error> {
        Err(Error::NotImplemented("get_table_types"))
    }

    fn commit(&self) -> Result<(), Self::Error> {
        Err(Error::NotImplemented("commit"))
    }

    fn rollback(&self) -> Result<(), Self::Error> {
        Err(Error::NotImplemented("commit"))
    }

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error> {
        Err(Error::NotImplemented("commit"))
    }
}

struct Statement {
    conn: Rc<Connection>,
}

impl AdbcStatement for Statement {
    type Error = Error;
    type ConnectionType = Connection;

    fn new_from_connection(conn: Rc<Self::ConnectionType>) -> Self {
        Self { conn }
    }

    fn set_option(&mut self, key: &str, value: &str) -> Result<(), Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn set_sql_query(&mut self, query: &str) -> Result<(), Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn set_substrait_plan(&mut self, query: &[u8]) -> Result<(), Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn get_param_schema(&self) -> Result<arrow::datatypes::Schema, Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn bind_data(&mut self, arr: arrow::array::ArrayRef) -> Result<(), Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn bind_stream(
        &mut self,
        stream: Box<dyn arrow::record_batch::RecordBatchReader>,
    ) -> Result<(), Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn prepare(&self) -> Result<(), Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn execute(&self) -> Result<arrow_adbc::interface::StatementResult, Self::Error> {
        Err(Error::NotImplemented(""))
    }

    fn execute_partitioned(
        &self,
    ) -> Result<arrow_adbc::interface::PartitionedStatementResult, Self::Error> {
        Err(Error::NotImplemented(""))
    }
}

arrow_adbc::interface::adbc_api!(Statement);
