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

//! Implementation of [`Extensions`].
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt::Debug,
};

/// Holds opaque extensions.
#[derive(Default)]
pub struct Extensions {
    inner: HashMap<TypeId, Box<dyn Extension>>,
}

impl Extensions {
    /// Create new, empty extensions collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set extensions by type.
    ///
    /// Returns existing extension if there was one.
    pub fn set<T>(&mut self, t: T) -> Option<T>
    where
        T: Clone + Debug + Send + Sync + 'static,
    {
        let ext = ExtensionImpl { inner: t };
        let existing = self.inner.insert(TypeId::of::<T>(), Box::new(ext));
        existing.map(|ext| *ext.to_any().downcast::<T>().expect("type ID is correct"))
    }

    /// Get immutable reference to extension by type.
    pub fn get<T>(&self) -> Option<&T>
    where
        T: Clone + Debug + Send + Sync + 'static,
    {
        self.inner.get(&TypeId::of::<T>()).map(|ext| {
            ext.as_any()
                .downcast_ref::<T>()
                .expect("type ID is correct")
        })
    }

    /// Get mutable reference to extension by type.
    pub fn get_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Clone + Debug + Send + Sync + 'static,
    {
        self.inner.get_mut(&TypeId::of::<T>()).map(|ext| {
            ext.as_any_mut()
                .downcast_mut::<T>()
                .expect("type ID is correct")
        })
    }

    /// Remove extension by type.
    pub fn remove<T>(&mut self) -> Option<T>
    where
        T: Clone + Debug + Send + Sync + 'static,
    {
        let existing = self.inner.remove(&TypeId::of::<T>());
        existing.map(|ext| *ext.to_any().downcast::<T>().expect("type ID is correct"))
    }
}

impl Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.inner.values()).finish()
    }
}

impl Clone for Extensions {
    fn clone(&self) -> Self {
        Self {
            inner: self
                .inner
                .iter()
                .map(|(ty_id, ext)| (*ty_id, ext.as_ref().clone()))
                .collect(),
        }
    }
}

/// Helper trait to capture relevant trait bounds for extensions into a vtable.
trait Extension: Debug + Send + Sync + 'static {
    /// Dyn-compatible [`Clone`].
    fn clone(&self) -> Box<dyn Extension>;

    /// Converts to boxed [`Any`].
    fn to_any(self: Box<Self>) -> Box<dyn Any>;

    /// Converts to [`Any`] reference.
    fn as_any(&self) -> &dyn Any;

    /// Converts to [`Any`] mutable reference.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Our one-and-only implementation of [`Extension`].
struct ExtensionImpl<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    inner: T,
}

impl<T> Debug for ExtensionImpl<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Extension for ExtensionImpl<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    fn clone(&self) -> Box<dyn Extension> {
        Box::new(Self {
            inner: self.inner.clone(),
        })
    }

    fn to_any(self: Box<Self>) -> Box<dyn Any> {
        let this = *self;
        Box::new(this.inner) as _
    }

    fn as_any(&self) -> &dyn Any {
        &self.inner
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extensions_traits() {
        let ext = Extensions::default();
        assert_send(&ext);
        assert_sync(&ext);
    }

    #[test]
    fn test_debug() {
        let mut ext = Extensions::default();
        ext.set(String::from("foo"));
        ext.set(1u8);

        let dbg_str = format!("{ext:?}");

        // order is NOT deterministic
        let variant_a = r#"{"foo", 1}"#;
        let variant_b = r#"{1, "foo"}"#;
        assert!(
            (dbg_str == variant_a) || (dbg_str == variant_b),
            "'{dbg_str}' is neither '{variant_a}' nor '{variant_b}'",
        );
    }

    #[test]
    fn test_get_set_remove() {
        let mut ext = Extensions::default();
        assert_eq!(ext.get::<String>(), None);
        assert_eq!(ext.get::<u8>(), None);
        assert_eq!(ext.get_mut::<String>(), None);
        assert_eq!(ext.get_mut::<u8>(), None);

        assert_eq!(ext.set(String::from("foo")), None);
        assert_eq!(ext.get::<String>(), Some(&String::from("foo")));
        assert_eq!(ext.get::<u8>(), None);
        assert_eq!(ext.get_mut::<String>(), Some(&mut String::from("foo")));
        assert_eq!(ext.get_mut::<u8>(), None);

        assert_eq!(ext.set(1u8), None);
        assert_eq!(ext.get::<String>(), Some(&String::from("foo")));
        assert_eq!(ext.get::<u8>(), Some(&1u8));
        assert_eq!(ext.get_mut::<String>(), Some(&mut String::from("foo")));
        assert_eq!(ext.get_mut::<u8>(), Some(&mut 1u8));

        assert_eq!(ext.set(String::from("bar")), Some(String::from("foo")));
        assert_eq!(ext.get::<String>(), Some(&String::from("bar")));
        assert_eq!(ext.get::<u8>(), Some(&1u8));
        assert_eq!(ext.get_mut::<String>(), Some(&mut String::from("bar")));
        assert_eq!(ext.get_mut::<u8>(), Some(&mut 1u8));

        ext.get_mut::<String>().unwrap().push_str("baz");
        assert_eq!(ext.get::<String>(), Some(&String::from("barbaz")));
        assert_eq!(ext.get::<u8>(), Some(&1u8));
        assert_eq!(ext.get_mut::<String>(), Some(&mut String::from("barbaz")));
        assert_eq!(ext.get_mut::<u8>(), Some(&mut 1u8));

        assert_eq!(ext.remove::<String>(), Some(String::from("barbaz")));
        assert_eq!(ext.get::<String>(), None);
        assert_eq!(ext.get::<u8>(), Some(&1u8));
        assert_eq!(ext.get_mut::<String>(), None);
        assert_eq!(ext.get_mut::<u8>(), Some(&mut 1u8));
        assert_eq!(ext.remove::<String>(), None);
    }

    #[test]
    fn test_clone() {
        let mut ext = Extensions::default();
        ext.set(String::from("foo"));

        let ext2 = ext.clone();

        ext.get_mut::<String>().unwrap().push_str("bar");
        ext.set(1u8);

        assert_eq!(ext.get::<String>(), Some(&String::from("foobar")));
        assert_eq!(ext.get::<u8>(), Some(&1));
        assert_eq!(ext2.get::<String>(), Some(&String::from("foo")));
        assert_eq!(ext2.get::<u8>(), None);
    }

    fn assert_send<T: Send>(_o: &T) {}
    fn assert_sync<T: Sync>(_o: &T) {}
}
