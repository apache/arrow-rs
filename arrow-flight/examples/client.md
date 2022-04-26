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

# client example

This app works against a running Flight Sql server.

Here are a couple of sample outputs:

```bash
cargo run --example=client --features="flight-sql-experimental"

arrow-flight 12.0.0
Apache Arrow <dev@arrow.apache.org>
Apache Arrow Flight

USAGE:
    client <SUBCOMMAND>

OPTIONS:
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    execute              
    get-catalogs         
    get-exported-keys    
    get-imported-keys    
    get-primary-keys     
    get-schemas          
    get-table-types      
    get-tables           
    help                 Print this message or the help of the given subcommand(s)
```

```
cargo run --example=client --features="flight-sql-experimental" -- execute --query "select * from app.inttable order by value desc"

+----+--------------+-------+-----------+
| ID | KEYNAME      | VALUE | FOREIGNID |
+----+--------------+-------+-----------+
| 1  | one          | 1     | 1         |
| 2  | zero         | 0     | 1         |
| 3  | negative one | -1    | 1         |
+----+--------------+-------+-----------+
+----+---------+-------+-----------+
| ID | KEYNAME | VALUE | FOREIGNID |
+----+---------+-------+-----------+
+----+---------+-------+-----------+
```

```
cargo run --example=client --features="flight-sql-experimental" -- get-tables --hostname localhost --port 52358

+--------------+----------------+------------------+--------------+
| catalog_name | db_schema_name | table_name       | table_type   |
+--------------+----------------+------------------+--------------+
|              | SYS            | SYSALIASES       | SYSTEM TABLE |
|              | SYS            | SYSCHECKS        | SYSTEM TABLE |
|              | SYS            | SYSCOLPERMS      | SYSTEM TABLE |
|              | SYS            | SYSCOLUMNS       | SYSTEM TABLE |
|              | SYS            | SYSCONGLOMERATES | SYSTEM TABLE |
|              | SYS            | SYSCONSTRAINTS   | SYSTEM TABLE |
|              | SYS            | SYSDEPENDS       | SYSTEM TABLE |
|              | SYS            | SYSFILES         | SYSTEM TABLE |
|              | SYS            | SYSFOREIGNKEYS   | SYSTEM TABLE |
|              | SYS            | SYSKEYS          | SYSTEM TABLE |
|              | SYS            | SYSPERMS         | SYSTEM TABLE |
|              | SYS            | SYSROLES         | SYSTEM TABLE |
|              | SYS            | SYSROUTINEPERMS  | SYSTEM TABLE |
|              | SYS            | SYSSCHEMAS       | SYSTEM TABLE |
|              | SYS            | SYSSEQUENCES     | SYSTEM TABLE |
|              | SYS            | SYSSTATEMENTS    | SYSTEM TABLE |
|              | SYS            | SYSSTATISTICS    | SYSTEM TABLE |
|              | SYS            | SYSTABLEPERMS    | SYSTEM TABLE |
|              | SYS            | SYSTABLES        | SYSTEM TABLE |
|              | SYS            | SYSTRIGGERS      | SYSTEM TABLE |
|              | SYS            | SYSUSERS         | SYSTEM TABLE |
|              | SYS            | SYSVIEWS         | SYSTEM TABLE |
|              | SYSIBM         | SYSDUMMY1        | SYSTEM TABLE |
|              | APP            | FOREIGNTABLE     | TABLE        |
|              | APP            | INTTABLE         | TABLE        |
+--------------+----------------+------------------+--------------+
```

