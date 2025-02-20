use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

/// Trait that must be implemented by extensions.
pub trait Extension: std::fmt::Debug + Send + Sync {
    /// Return a &Any for this type.
    fn as_any(self: Arc<Self>) -> Ext;

    /// Ensure that [`Extensions`] can implement [`std::cmp::PartialEq`] by requiring [`Extension`]
    /// implementors to implement a dyn-compatible partial equality operation.
    ///
    /// This is necessary because [`std::cmp::PartialEq`] uses a `Self` type parameter, which
    /// violates dyn-compatibility rules:
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

    fn set_ext_wrapper<T: Any + Send + Sync + std::fmt::Debug + 'static>(
        &mut self,
        t: ExtWrapper<T>,
    ) {
        let id = TypeId::of::<T>();
        // println!("inserting: {id:?}: {}", std::any::type_name::<T>());
        let a = Arc::new(t);
        self.inner.insert(id, a);
    }

    pub(crate) fn get_ext<T: Any + Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let id = TypeId::of::<T>();
        // println!(
        //     "looking for        : {id:?}: {}",
        //     std::any::type_name::<T>()
        // );
        self.inner.get(&id).map(|e| {
            // let id = e.type_id();
            // println!(
            //     "found value        : {id:?}: {}",
            //     std::any::type_name_of_val(e)
            // );
            let v = Arc::clone(&e);
            // let id = v.type_id();
            // println!(
            //     "cloned value       : {id:?}: {}",
            //     std::any::type_name_of_val(&v)
            // );
            let v = v.as_any();
            // let id = v.type_id();
            // println!(
            //     "as_any value       : {id:?}: {}",
            //     std::any::type_name_of_val(&v)
            // );
            let v: Arc<T> =
                Arc::downcast::<T>(v).expect("must be able to downcast to type if found in map");
            // let id = v.type_id();
            // println!(
            //     "found value        : {id:?}: {}",
            //     std::any::type_name_of_val(&v)
            // );
            // let id = v.as_ref().type_id();
            // println!(
            //     "found value (inner): {id:?}: {}",
            //     std::any::type_name_of_val(v.as_ref())
            // );
            v
        })
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
///
/// NOTE: Different instances of the same type held by ExtWrapper are considered equal for the sake
/// of the `Extensions.partial_eq` trait since its intended use case is for wrapping dyn-compatible
/// trait objects; because [`std::cmp::PartialEq`] has `Self` as a generic type parameter, the type
/// system won't let us set it as a trait bound on incoming trait objects when constructing
/// ExtWrapper.
#[derive(Debug)]
struct ExtWrapper<T> {
    inner: Arc<T>,
}

impl<T: std::fmt::Debug + Send + Sync> ExtWrapper<T> {
    fn new(v: T) -> Self {
        Self { inner: Arc::new(v) }
    }
}

impl<T: std::fmt::Debug + Send + Sync + 'static> Extension for ExtWrapper<T> {
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
            let self_id = self.type_id();
            let other_id = other.type_id();
            println!("MyExt.partial_eq");
            println!(
                "  self  : {self_id:?}, {}",
                std::any::type_name_of_val(self.as_ref())
            );
            println!(
                "  other : {other_id:?}, {}",
                std::any::type_name_of_val(other.as_ref())
            );
            other
                .downcast_ref::<Self>()
                .inspect(|v| println!("{v:?}"))
                .map(|other| self.x == other.x)
                .unwrap_or_default()
        }
    }

    #[test]
    fn equality_custom_ext_trait_impl() {
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
    fn equality_ext_wrapper() {
        let mut exts1 = Extensions::default();
        let wrapped1 = ExtWrapper::new(0);
        let t1 = wrapped1.type_id();
        println!("type id of ExtWrapper<i32>: {t1:?}");
        exts1.set_ext_wrapper(wrapped1);

        let mut exts2 = Extensions::default();
        let wrapped2 = ExtWrapper::new(1);
        let t2 = wrapped2.type_id();
        println!("type id of ExtWrapper<i32>: {t2:?}");

        exts2.set_ext_wrapper(wrapped2);

        assert_eq!(
            exts1, exts2,
            // this behavior is necessary because ExtWrapper is a generic impl of Extensions
            // necessary for converting from external Arc<dyn Any ...> types where none of the
            // trait bounts can implement PartialEq due to dyn-compatibility requirements.
            "extensions of type ExtWrapper are always equal even if their values aren't"
        );

        let mut exts3 = Extensions::default();
        let wrapped3 = ExtWrapper::new(0);
        exts3.set_ext_wrapper(wrapped3);

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

        println!("validate ExtWrapper<i32> with value 0");
        let wrapped = ExtWrapper::new(0i32);
        exts.set_ext_wrapper(wrapped);
        let expected = exts.get_ext::<i32>().expect("must get a value");
        assert_eq!(0, *expected, "return the same instance we just added");

        println!("validate replacing previous ExtWrapper<i32> with value 1");
        let wrapped = ExtWrapper::new(1i32);
        exts.set_ext_wrapper(wrapped);
        let expected = exts.get_ext::<i32>().expect("must get a value");
        assert_eq!(1, *expected, "return the same instance we just added");
    }
}
