use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

/// Trait that must be implemented by extensions.
pub trait Extension: std::fmt::Debug + Send + Sync {
    /// Return a &Any for this type.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Ensure that [`Extensions`] can implement [`std::cmp::PartialEq`] by requiring [`ExtTrait`]
    /// implementors to implement a dyn-compatible partial equality operation.
    ///
    /// This is necessary because [`std::cmp::PartialEq`] uses a `Self` type parameter, which
    /// violates dyn-compatibility rules: https://doc.rust-lang.org/error_codes/E0038.html#trait-uses-self-as-a-type-parameter-in-the-supertrait-listing
    fn partial_eq(&self, other: &dyn std::any::Any) -> bool;
}

type ExtensionMap = HashMap<TypeId, Arc<dyn Extension>>;
type Ext = Arc<dyn Any + Send + Sync + 'static>;

/// Type that holds opaque instances of types referenced by their [`std::any::TypeId`].
///
/// Types are stored as instances of the [`Extension`] trait in order to enable implementation of
/// [`std::fmt::Debug`] and [`std::cmp::PartialEq`].
#[derive(Debug, Default, Clone)]
pub struct Extensions {
    inner: ExtensionMap,
}

impl PartialEq for Extensions {
    fn eq(&self, other: &Self) -> bool {
        for (k, v) in &self.inner {
            if let Some(ov) = other.inner.get(&k) {
                if !v.partial_eq(ov.as_any()) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

impl Extensions {
    pub(crate) fn set_ext<T: Extension + 'static>(&mut self, t: T) {
        let id = t.type_id();
        let a = Arc::new(t);
        self.inner.insert(id, a);
    }

    pub(crate) fn get_ext<T: Extension + 'static>(&self) -> Option<&T> {
        let id = TypeId::of::<T>();
        self.inner
            .get(&id)
            .map(|e| e.as_any().downcast_ref())
            .flatten()
    }
}

impl From<HashMap<TypeId, Ext>> for Extensions {
    fn from(other: HashMap<TypeId, Ext>) -> Self {
        let mut s = Self::default();
        for (k, v) in other {
            let v = Arc::new(ExtWrapper::new(v));
            s.inner.insert(k, v);
        }
        s
    }
}

/// Type that implements [`Extension`] for the sake of converting to [`Extensions`] from external
/// instances of `HashMap<TypeId, Ext>'.
#[derive(Debug)]
struct ExtWrapper<T: std::fmt::Debug> {
    inner: Arc<T>,
}

impl<T: std::fmt::Debug> ExtWrapper<T> {
    fn new(v: T) -> Self {
        Self { inner: Arc::new(v) }
    }
}

impl<T: Any + Send + Sync + std::fmt::Debug + 'static> Extension for ExtWrapper<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_ref()
    }

    fn partial_eq(&self, other: &dyn std::any::Any) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct MyExt {
        x: u8,
    }

    impl Extension for MyExt {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn partial_eq(&self, other: &dyn std::any::Any) -> bool {
            let self_id = self.type_id();
            let other_id = other.type_id();
            println!("MyExt.partial_eq");
            println!(
                "  self  : {self_id:?}, {}",
                std::any::type_name_of_val(self)
            );
            println!(
                "  other : {other_id:?}, {}",
                std::any::type_name_of_val(other)
            );
            other
                .downcast_ref::<Self>()
                .inspect(|v| println!("{v:?}"))
                .map(|other| self.x == other.x)
                .unwrap_or_default()
        }
    }

    #[test]
    fn custom_ext_trait_impl() {
        let mut exts1 = Extensions::default();
        let myext1 = MyExt { x: 0 };
        exts1.set_ext(myext1);
        let t1 = TypeId::of::<MyExt>();
        println!("type id of MyExt: {t1:?}");

        let mut exts2 = Extensions::default();
        let myext2 = MyExt { x: 1 };
        exts2.set_ext(myext2);

        assert_ne!(
            exts1, exts2,
            "extensions of the same type with different values should not be equal"
        );

        let mut exts3 = Extensions::default();
        let myext3 = MyExt { x: 0 };
        exts3.set_ext(myext3);

        assert_eq!(
            exts1, exts3,
            "extensions of the same type with the same values should be equal"
        );
    }

    #[test]
    fn ext_wrapper() {
        let v1 = 0;
        let v2 = 1;
        let v3 = 0;

        let wrapped1 = ExtWrapper::new(v1);
        let wrapped2 = ExtWrapper::new(v2);
        let wrapped3 = ExtWrapper::new(v3);

        let t1 = wrapped1.type_id();
        println!("type id of ExtWrapper<i32>: {t1:?}");

        let t2 = wrapped2.type_id();
        println!("type id of ExtWrapper<i32>: {t2:?}");

        let mut exts1 = Extensions::default();
        exts1.set_ext(wrapped1);

        let mut exts2 = Extensions::default();
        exts2.set_ext(wrapped2);

        assert_eq!(
            exts1, exts2,
            "extentions of type ExtWrapper are always equal even if their values aren't"
        );

        let mut exts3 = Extensions::default();
        exts3.set_ext(wrapped3);

        assert_eq!(exts1, exts3, "extentions of type ExtWrapper are equal");
    }
}
