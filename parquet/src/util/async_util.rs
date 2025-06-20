#[cfg(feature = "async-no-send")]
mod send_impl {
    pub trait MaybeSend {}
    impl<T: ?Sized> MaybeSend for T {}
    pub type MaybeLocalBoxFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;
}

#[cfg(not(feature = "async-no-send"))]
mod send_impl {
    pub use std::marker::Send as MaybeSend;
    pub type MaybeLocalBoxFuture<'a, T> = futures::future::BoxFuture<'a, T>;
}

pub use send_impl::*;

pub trait MaybeLocalFutureExt: std::future::Future {
    fn boxed_maybe_local<'a>(self) -> MaybeLocalBoxFuture<'a, Self::Output>
    where
        Self: Sized + MaybeSend + 'a,
    {
        Box::pin(self)
    }
}

impl<T> MaybeLocalFutureExt for T where T: std::future::Future {}
