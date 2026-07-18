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

/// Parses a dot-separated Parquet column path, using `\` as an escape character
/// for literal dots and backslashes.
pub fn parse_column_path(path: &str) -> Result<Vec<String>, String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut escaped = false;

    for c in path.chars() {
        if escaped {
            match c {
                '.' | '\\' => current.push(c),
                _ => {
                    return Err(format!(
                        "Invalid escape sequence \\{c} in column path {}",
                        path
                    ));
                }
            }
            escaped = false;
            continue;
        }

        match c {
            '\\' => escaped = true,
            '.' => {
                parts.push(current);
                current = String::new();
            }
            _ => current.push(c),
        }
    }

    if escaped {
        return Err(format!(
            "Column path {} ends with an incomplete escape sequence",
            path
        ));
    }

    parts.push(current);
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_column_path_splits_unescaped_dots() {
        assert_eq!(parse_column_path("a.b.c").unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_column_path_allows_escaped_dots() {
        assert_eq!(parse_column_path(r"a\.b.c").unwrap(), vec!["a.b", "c"]);
    }

    #[test]
    fn parse_column_path_allows_escaped_backslashes() {
        assert_eq!(parse_column_path(r"a\\b.c").unwrap(), vec![r"a\b", "c"]);
    }

    #[test]
    fn parse_column_path_invalid_escape_sequence() {
        let err = parse_column_path(r"a.\b.c").unwrap_err();
        assert!(err.contains("Invalid escape sequence"));
    }

    #[test]
    fn parse_column_path_incomplete_escape_sequence() {
        let err = parse_column_path(r"a.b.c\").unwrap_err();
        assert!(err.contains("incomplete escape sequence"));
    }
}
