use crate::alloc;
use crate::array::{ArrayData, PrimitiveArray};
use crate::buffer::Buffer;
use crate::datatypes::ArrowPrimitiveType;
use std::iter::FromIterator;
use std::mem;
use std::mem::ManuallyDrop;

/// A `Vec` wrapper with a memory alignment equal to Arrow's primitive arrays.
/// Can be useful in creating Arrow Buffers with Vec semantics
#[derive(Debug)]
pub struct AlignedVec<T> {
    inner: Vec<T>,
    // if into_inner is called, this will be true and we can use the default Vec's destructor
    taken: bool,
}

impl<T> Drop for AlignedVec<T> {
    fn drop(&mut self) {
        if !self.taken {
            let inner = mem::take(&mut self.inner);
            let mut me = mem::ManuallyDrop::new(inner);
            let ptr: *mut T = me.as_mut_ptr();
            let ptr = ptr as *mut u8;
            let ptr = std::ptr::NonNull::new(ptr).unwrap();
            unsafe { alloc::free_aligned::<u8>(ptr, self.capacity()) }
        }
    }
}

impl<T> FromIterator<T> for AlignedVec<T> {
    /// Create AlignedVec from Iterator.
    ///
    /// # Panic
    ///
    /// The iterators length size hint must be correct, or else this will panic.
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let sh = iter.size_hint();
        let size = sh.1.unwrap_or(sh.0);

        let mut av = Self::with_capacity(size);
        av.extend(iter);

        // Iterator size hint wasn't correct and reallocation has occurred
        assert!(av.len() <= size);
        av
    }
}

impl<T: Copy> AlignedVec<T> {
    /// Create a new AlignedVec
    ///
    /// Uses a memcpy to initialize this AlignedVec
    pub fn new_from_slice(other: &[T]) -> Self {
        let len = other.len();
        let mut av = Self::with_capacity(len);
        unsafe {
            // Safety:
            // we set initiate the memory after this with a memcpy.
            av.set_len(len);
        }
        av.inner.copy_from_slice(other);
        av
    }
}

impl<T: Clone> AlignedVec<T> {
    /// Resizes the `Vec` in-place so that `len` is equal to `new_len`.
    ///
    /// If `new_len` is greater than `len`, the `Vec` is extended by the
    /// difference, with each additional slot filled with `value`.
    /// If `new_len` is less than `len`, the `Vec` is simply truncated.
    ///
    /// This method requires `T` to implement [`Clone`],
    /// in order to be able to clone the passed value.
    pub fn resize(&mut self, new_len: usize, value: T) {
        self.inner.resize(new_len, value)
    }

    /// Clones and appends all elements in a slice to the `Vec`.
    ///
    /// Iterates over the slice `other`, clones each element, and then appends
    /// it to this `Vec`. The `other` vector is traversed in-order.
    pub fn extend_from_slice(&mut self, other: &[T]) {
        let remaining_cap = self.capacity() - self.len();
        let needed_cap = other.len();
        // exponential allocation
        if needed_cap > remaining_cap {
            self.reserve(std::cmp::max(needed_cap, self.capacity()));
        }
        self.inner.extend_from_slice(other)
    }
}

impl<T> AlignedVec<T> {
    /// Constructs a new, empty `AlignedVec<T>`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new Vec where first bytes memory address has an alignment of 64 bytes, as described
    /// by arrow spec.
    pub fn with_capacity(size: usize) -> Self {
        // Can only have a zero copy to arrow memory if address of first byte % 64 == 0
        let t_size = std::mem::size_of::<T>();
        let capacity = size * t_size;
        let ptr = alloc::allocate_aligned::<u8>(capacity).as_ptr() as *mut T;
        let v = unsafe { Vec::from_raw_parts(ptr, 0, size) };
        AlignedVec {
            inner: v,
            taken: false,
        }
    }

    /// Returns `true` if the vector contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Reserves capacity for at least `additional` more elements to be inserted
    /// in the given `Vec<T>`. The collection may reserve more space to avoid
    /// frequent reallocations. After calling `reserve`, capacity will be
    /// greater than or equal to `self.len() + additional`. Does nothing if
    /// capacity is already sufficient.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX` bytes.
    pub fn reserve(&mut self, additional: usize) {
        let mut me = ManuallyDrop::new(mem::take(&mut self.inner));
        let ptr = me.as_mut_ptr() as *mut u8;
        let ptr = std::ptr::NonNull::new(ptr).unwrap();
        let t_size = mem::size_of::<T>();
        let cap = me.capacity();
        let old_capacity = t_size * cap;
        let new_capacity = old_capacity + t_size * additional;
        let ptr = unsafe { alloc::reallocate::<u8>(ptr, old_capacity, new_capacity) };
        let ptr = ptr.as_ptr() as *mut T;
        let v = unsafe { Vec::from_raw_parts(ptr, me.len(), cap + additional) };
        self.inner = v;
    }

    /// Returns the number of elements in the vector, also referred to
    /// as its 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Create a new aligned vec from a ptr.
    ///
    /// # Safety
    /// The ptr should be 64 byte aligned and `len` and `capacity` should be correct otherwise it is UB.
    pub unsafe fn from_ptr(ptr: usize, len: usize, capacity: usize) -> Self {
        assert_eq!((ptr as usize) % alloc::ALIGNMENT, 0);
        let ptr = ptr as *mut T;
        let v = Vec::from_raw_parts(ptr, len, capacity);
        Self {
            inner: v,
            taken: false,
        }
    }

    /// Take ownership of the Vec. This is UB because the destructor of Vec<T> probably has a different
    /// alignment than what we allocated.
    unsafe fn into_inner(mut self) -> Vec<T> {
        self.taken = true;
        mem::take(&mut self.inner)
    }

    /// Push at the end of the Vec.
    #[inline]
    pub fn push(&mut self, value: T) {
        // Make sure that we always reallocate, and not let the inner vec reallocate!
        // otherwise the wrong deallocator will run.
        if self.inner.len() == self.capacity() {
            // exponential allocation
            self.reserve(std::cmp::max(self.capacity(), 5));
        }
        self.inner.push(value)
    }

    /// Set the length of the underlying `Vec`.
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to `capacity`.
    /// - The elements at `old_len..new_len` must be initialized.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        self.inner.set_len(new_len);
    }

    /// Returns a raw pointer to the vector's buffer.
    pub fn as_ptr(&self) -> *const T {
        self.inner.as_ptr()
    }

    /// Returns an unsafe mutable pointer to the vector's buffer.
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.inner.as_mut_ptr()
    }

    /// Extracts a mutable slice of the entire vector.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }

    /// Returns the number of elements the vector can hold without
    /// reallocating.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Decomposes a `Vec<T>` into its raw components.
    pub fn into_raw_parts(self) -> (*mut T, usize, usize) {
        let mut me = ManuallyDrop::new(self);
        (me.as_mut_ptr(), me.len(), me.capacity())
    }

    /// Shrinks the capacity of the vector as much as possible.
    pub fn shrink_to_fit(&mut self) {
        if self.capacity() > self.len() {
            let mut me = ManuallyDrop::new(mem::take(&mut self.inner));
            let ptr = me.as_mut_ptr() as *mut u8;
            let ptr = std::ptr::NonNull::new(ptr).unwrap();

            let t_size = mem::size_of::<T>();
            let new_size = t_size * me.len();
            let old_size = t_size * me.capacity();
            let v = unsafe {
                let ptr =
                    alloc::reallocate::<u8>(ptr, old_size, new_size).as_ptr() as *mut T;
                Vec::from_raw_parts(ptr, me.len(), me.len())
            };

            self.inner = v;
        }
    }

    /// Transform this array to an Arrow Buffer.
    pub fn into_buffer(self) -> Buffer {
        let values = unsafe { self.into_inner() };

        let me = mem::ManuallyDrop::new(values);
        let ptr = me.as_ptr() as *mut u8;
        let len = me.len() * std::mem::size_of::<T>();
        let capacity = me.capacity() * std::mem::size_of::<T>();
        debug_assert_eq!((ptr as usize) % 64, 0);
        let ptr = std::ptr::NonNull::new(ptr).unwrap();

        unsafe { Buffer::from_raw_parts(ptr, len, capacity) }
    }

    /// Transform this Vector into a PrimitiveArray
    pub fn into_primitive_array<A: ArrowPrimitiveType>(
        self,
        null_buf: Option<Buffer>,
    ) -> PrimitiveArray<A> {
        debug_assert_eq!(mem::size_of::<A::Native>(), mem::size_of::<T>());

        let vec_len = self.len();
        let buffer = self.into_buffer();

        let mut builder = ArrayData::builder(A::DATA_TYPE)
            .len(vec_len)
            .add_buffer(buffer);

        if let Some(buf) = null_buf {
            builder = builder.null_bit_buffer(buf);
        }
        let data = builder.build();

        PrimitiveArray::<A>::from(data)
    }

    /// Extend the AlignedVec by the contents of the iterator.
    ///
    /// # Panic
    ///
    /// Must be a trusted len iterator or else it will panic
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        let cap = iter.size_hint().1.expect("a trusted length iterator");
        let (extra_cap, overflow) = cap.overflowing_sub(self.capacity());
        if extra_cap > 0 && !overflow {
            self.reserve(extra_cap);
        }
        let len_before = self.len();
        self.inner.extend(iter);
        let added = self.len() - len_before;
        assert_eq!(added, cap)
    }
}

impl<T> Default for AlignedVec<T> {
    fn default() -> Self {
        // Be careful here. Don't initialize with a normal Vec as this will cause the wrong deallocator
        // to run and SIGSEGV
        Self::with_capacity(0)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::datatypes::Int32Type;

    #[test]
    fn test_aligned_vec_allocations() {
        // Can only have a zero copy to arrow memory if address of first byte % 64 == 0
        // check if we can increase above initial capacity and keep the Arrow alignment
        let mut v = AlignedVec::with_capacity(2);
        v.push(1);
        v.push(2);
        v.push(3);
        v.push(4);

        let ptr = v.as_ptr();
        assert_eq!((ptr as usize) % alloc::ALIGNMENT, 0);

        // check if we can shrink to fit
        let mut v = AlignedVec::with_capacity(10);
        v.push(1);
        v.push(2);
        v.shrink_to_fit();
        assert_eq!(v.len(), 2);
        assert_eq!(v.capacity(), 2);
        let ptr = v.as_ptr();
        assert_eq!((ptr as usize) % alloc::ALIGNMENT, 0);

        let a = v.into_primitive_array::<Int32Type>(None);
        assert_eq!(&a.values()[..2], &[1, 2])
    }
}
