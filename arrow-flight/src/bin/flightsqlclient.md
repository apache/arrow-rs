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

# Flight SQL Client

flightsqlclient is a small command line app to query a [Flight SQL server](https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/).
You can find instructions to run such a server here: [notes on running java FlightSqlExample](http://timvw.be/2022/04/28/notes-on-running-java-flightsqlexample/).

When you run the app without arguments, you will be greeted with it's usage:

```bash
cargo run --bin=flightsqlclient --features="flight-sql-experimental"

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

Listing the available tables can be done as following:

```
cargo run --bin=flightsqlclient --features="flight-sql-experimental" -- get-tables --hostname localhost --port 52358

+--------------+----------------+------------------+--------------+
| catalog_name | db_schema_name | table_name       | table_type   |
+--------------+----------------+------------------+--------------+
|              | SYS            | SYSALIASES       | SYSTEM TABLE |
... (removed some lines to reduce output)
|              | APP            | FOREIGNTABLE     | TABLE        |
|              | APP            | INTTABLE         | TABLE        |
+--------------+----------------+------------------+--------------+
```

A query can be executed as following:

```
cargo run --bin=flightsqlclient --features="flight-sql-experimental" -- execute --query "select * from app.inttable order by value desc"

+----+--------------+-------+-----------+
| ID | KEYNAME      | VALUE | FOREIGNID |
+----+--------------+-------+-----------+
| 1  | one          | 1     | 1         |
| 2  | zero         | 0     | 1         |
| 3  | negative one | -1    | 1         |
+----+--------------+-------+-----------+
```



