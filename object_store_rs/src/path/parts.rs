use percent_encoding::{percent_decode, percent_encode, AsciiSet, CONTROLS};
use std::borrow::Cow;

use crate::path::DELIMITER_BYTE;
use snafu::Snafu;

/// Error returned by [`PathPart::parse`]
#[derive(Debug, Snafu)]
#[snafu(display("Invalid path segment - got \"{}\" expected: \"{}\"", actual, expected))]
#[allow(missing_copy_implementations)]
pub struct InvalidPart {
    actual: String,
    expected: String,
}

/// The PathPart type exists to validate the directory/file names that form part
/// of a path.
///
/// A PathPart instance is guaranteed to to contain no illegal characters (e.g. `/`)
/// as it can only be constructed by going through the `from` impl.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct PathPart<'a> {
    pub(super) raw: Cow<'a, str>,
}

impl<'a> PathPart<'a> {
    /// Parse the provided path segment as a [`PathPart`] returning an error if invalid
    pub fn parse(segment: &'a str) -> Result<Self, InvalidPart> {
        let decoded: Cow<'a, [u8]> = percent_decode(segment.as_bytes()).into();
        let part = PathPart::from(decoded.as_ref());
        if segment != part.as_ref() {
            return Err(InvalidPart {
                actual: segment.to_string(),
                expected: part.raw.to_string(),
            });
        }

        Ok(Self {
            raw: segment.into(),
        })
    }
}

/// Characters we want to encode.
const INVALID: &AsciiSet = &CONTROLS
    // The delimiter we are reserving for internal hierarchy
    .add(DELIMITER_BYTE)
    // Characters AWS recommends avoiding for object keys
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
    .add(b'\\')
    .add(b'{')
    .add(b'^')
    .add(b'}')
    .add(b'%')
    .add(b'`')
    .add(b']')
    .add(b'"') // " <-- my editor is confused about double quotes within single quotes
    .add(b'>')
    .add(b'[')
    .add(b'~')
    .add(b'<')
    .add(b'#')
    .add(b'|')
    // Characters Google Cloud Storage recommends avoiding for object names
    // https://cloud.google.com/storage/docs/naming-objects
    .add(b'\r')
    .add(b'\n')
    .add(b'*')
    .add(b'?');

impl<'a> From<&'a [u8]> for PathPart<'a> {
    fn from(v: &'a [u8]) -> Self {
        let inner = match v {
            // We don't want to encode `.` generally, but we do want to disallow parts of paths
            // to be equal to `.` or `..` to prevent file system traversal shenanigans.
            b"." => "%2E".into(),
            b".." => "%2E%2E".into(),
            other => percent_encode(other, INVALID).into(),
        };
        Self { raw: inner }
    }
}

impl<'a> From<&'a str> for PathPart<'a> {
    fn from(v: &'a str) -> Self {
        Self::from(v.as_bytes())
    }
}

impl From<String> for PathPart<'static> {
    fn from(s: String) -> Self {
        Self {
            raw: Cow::Owned(PathPart::from(s.as_str()).raw.into_owned()),
        }
    }
}

impl<'a> AsRef<str> for PathPart<'a> {
    fn as_ref(&self) -> &str {
        self.raw.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_part_delimiter_gets_encoded() {
        let part: PathPart<'_> = "foo/bar".into();
        assert_eq!(part.raw, "foo%2Fbar");
    }

    #[test]
    fn path_part_given_already_encoded_string() {
        let part: PathPart<'_> = "foo%2Fbar".into();
        assert_eq!(part.raw, "foo%252Fbar");
    }

    #[test]
    fn path_part_cant_be_one_dot() {
        let part: PathPart<'_> = ".".into();
        assert_eq!(part.raw, "%2E");
    }

    #[test]
    fn path_part_cant_be_two_dots() {
        let part: PathPart<'_> = "..".into();
        assert_eq!(part.raw, "%2E%2E");
    }
}
