use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

/// Trait that must be implemented by extensions.
pub trait Extension: std::fmt::Debug + Send + Sync {
    /// Return a `Arc<dyn Any + Send + Sync + 'static>` for this type.
    fn as_any(self: Arc<Self>) -> Ext;

    /// Ensure that [`Extensions`] can implement [`std::cmp::PartialEq`] by requiring [`Extension`]
    /// implementors to implement a dyn-compatible partial equality operation.
    ///
    /// This is necessary rather than using a [`std::cmp::PartialEq`] trait bound because uses a
    /// `Self` type parameter, which violates dyn-compatibility rules:
    /// https://doc.rust-lang.org/error_codes/E0038.html#trait-uses-self-as-a-type-parameter-in-the-supertrait-listing
    fn partial_eq(self: Arc<Self>, other: Ext) -> bool;
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
                if !v.clone().partial_eq(ov.clone().as_any()) {
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

    pub(crate) fn get_ext<T: Any + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let id = TypeId::of::<T>();
        self.inner.get(&id).map(|e| {
            let v = Arc::clone(&e).as_any();
            Arc::downcast::<T>(v).unwrap()
        })
    }
}

impl From<HashMap<TypeId, Ext>> for Extensions {
    fn from(other: HashMap<TypeId, Ext>) -> Self {
        let mut s = Self::default();
        for (k, v) in other {
            let v = Arc::new(ExtWrapper { inner: v });
            s.inner.insert(k, v);
        }
        s
    }
}

/// Type that implements [`Extension`] for the sake of converting to [`Extensions`] from external
/// instances of `HashMap<TypeId, Ext>'.
///
/// NOTE: Different instances of the same type held by ExtWrapper are considered equal for the sake
/// of the `Extensions.partial_eq` trait since its intended use case is for wrapping dyn-compatible
/// trait objects; because [`std::cmp::PartialEq`] has `Self` as a generic type parameter, the type
/// system won't let us set it as a trait bound on incoming trait objects when constructing
/// ExtWrapper.
struct ExtWrapper {
    inner: Arc<dyn Any + Send + Sync + 'static>,
}

impl std::fmt::Debug for ExtWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExtWrapper2")
    }
}

impl Extension for ExtWrapper {
    fn as_any(self: Arc<Self>) -> Ext {
        self.inner.clone()
    }

    fn partial_eq(self: Arc<Self>, _: Ext) -> bool {
        // this is necessary because ExtWrapper is a generic impl of Extensions necessary for
        // converting from external Arc<dyn Any ...> types where none of the trait bounts can
        // implement PartialEq due to dyn-compatibility requirements.
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
        fn as_any(self: Arc<Self>) -> Ext {
            self
        }

        fn partial_eq(self: Arc<Self>, other: Ext) -> bool {
            other
                .downcast_ref::<Self>()
                .map(|other| self.x == other.x)
                .unwrap_or_default()
        }
    }

    #[test]
    fn equality_custom_ext_trait_impl() {
        let mut exts1 = Extensions::default();
        let myext1 = MyExt { x: 0 };
        exts1.set_ext(myext1);

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

    impl Extensions {
        fn set_ext_wrapped<T: Any + Send + Sync + std::fmt::Debug + 'static>(&mut self, t: T) {
            let id = TypeId::of::<T>();
            let a = Arc::new(ExtWrapper { inner: Arc::new(t) });
            self.inner.insert(id, a);
        }
    }

    #[test]
    fn not_equal_if_missing_entry() {
        let mut exts1 = Extensions::default();
        exts1.set_ext_wrapped(0);
        exts1.set_ext_wrapped(String::from("meow"));

        let mut exts2 = Extensions::default();
        exts2.set_ext_wrapped(1);

        assert_ne!(
            exts1, exts2,
            "two different Extensions cannot be equal if they don't carry the same types"
        );
    }
    #[test]
    fn equality_ext_wrapper() {
        let mut exts1 = Extensions::default();
        exts1.set_ext_wrapped(0);

        let mut exts2 = Extensions::default();
        exts2.set_ext_wrapped(1);

        assert_eq!(
            exts1, exts2,
            // this behavior is necessary because ExtWrapper is a generic impl of Extensions
            // necessary for converting from external Arc<dyn Any ...> types where none of the
            // trait bounts can implement PartialEq due to dyn-compatibility requirements.
            "extensions of type ExtWrapper are always equal even if their values aren't"
        );

        let mut exts3 = Extensions::default();
        exts3.set_ext_wrapped(0);

        assert_eq!(exts1, exts3, "extensions of type ExtWrapper are equal");
    }

    #[test]
    fn one_instance_of_same_type_custom_impl() {
        let mut exts = Extensions::default();

        println!("validate MyExt");
        let myext = MyExt { x: 0 };
        exts.set_ext(myext);
        let expected = exts.get_ext::<MyExt>().expect("must get a value");
        assert_eq!(0, expected.x, "return the same instance we just added");

        println!("validate replacing previous MyExt");
        let myext = MyExt { x: 1 };
        exts.set_ext(myext);
        let expected = exts.get_ext::<MyExt>().expect("must get a value");
        assert_eq!(1, expected.x, "return the same instance we just added");
    }

    #[test]
    fn one_instance_of_same_type_ext_wrapper() {
        let mut exts = Extensions::default();

        println!("validate ExtWrapper with value 0");
        exts.set_ext_wrapped(0i32);
        let expected = exts.get_ext::<i32>().expect("must get a value");
        assert_eq!(0, *expected, "return the same instance we just added");

        println!("validate replacing previous ExtWrapper with value 1");
        exts.set_ext_wrapped(1i32);
        let expected = exts.get_ext::<i32>().expect("must get a value");
        assert_eq!(1, *expected, "return the same instance we just added");
    }

    #[test]
    fn from_hashmap_of_exts() {
        let mut map: HashMap<TypeId, Ext> = HashMap::new();

        let v = 0i32;
        let id = v.type_id();
        assert!(map.insert(id, Arc::new(v)).is_none());

        let s: String = "meow".to_string();
        let id = s.type_id();
        assert!(map.insert(id, Arc::new(s)).is_none());

        assert!(map.len() == 2);

        let exts: Extensions = map.into();

        let v = exts.get_ext::<i32>().expect("must get a value");
        assert_eq!(0i32, *v.as_ref());

        let v = exts.get_ext::<String>().expect("must get a value");
        assert_eq!(String::from("meow"), *v.as_ref());
    }
}
