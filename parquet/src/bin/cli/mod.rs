// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
                        "Invalid escape sequence \\{c} in column path {path}"
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
            "Column path {path} ends with an incomplete escape sequence"
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
