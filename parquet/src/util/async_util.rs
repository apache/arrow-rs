#[cfg(feature = "async-no-send")]
mod send_impl {
    pub trait Send {}
    impl<T> Send for T {}
    pub type BoxFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;
}

#[cfg(not(feature = "async-no-send"))]
mod send_impl {
    pub trait Send: std::marker::Send {}
    impl<T> Send for T where T: std::marker::Send {}
    pub type BoxFuture<'a, T> = futures::future::BoxFuture<'a, T>;
}

pub use send_impl::*;
