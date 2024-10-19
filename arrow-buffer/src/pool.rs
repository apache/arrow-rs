use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A [`MemoryPool`] can be used to track memory usage by [`Buffer`](crate::Buffer)
pub trait MemoryPool {
    fn register(&self, size: usize) -> Box<dyn MemoryReservation>;
}

/// A memory reservation within a [`MemoryPool`] that is freed on drop
pub trait MemoryReservation {
    fn resize(&mut self, new: usize);
}

/// A simple [`MemoryPool`] that tracks memory usage
#[derive(Debug, Default)]
pub struct TrackingMemoryPool(Arc<AtomicUsize>);

impl TrackingMemoryPool {
    /// Returns the total allocated size
    pub fn allocated(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

impl MemoryPool for TrackingMemoryPool {
    fn register(&self, size: usize) -> Box<dyn MemoryReservation> {
        self.0.fetch_add(size, Ordering::Relaxed);
        Box::new(Tracker {
            size,
            shared: Arc::clone(&self.0),
        })
    }
}

#[derive(Debug)]
struct Tracker {
    size: usize,
    shared: Arc<AtomicUsize>,
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.shared.fetch_sub(self.size, Ordering::Relaxed);
    }
}

impl MemoryReservation for Tracker {
    fn resize(&mut self, new: usize) {
        match self.size < new {
            true => self.shared.fetch_add(new - self.size, Ordering::Relaxed),
            false => self.shared.fetch_sub(self.size - new, Ordering::Relaxed),
        };
        self.size = new;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Buffer;

    #[test]
    fn test_memory_pool() {
        let pool = TrackingMemoryPool::default();
        let b1 = Buffer::from(vec![0_i64, 1, 2]);
        let b2 = Buffer::from(vec![3_u16, 4, 5]);

        let buffers = [b1.clone(), b1.slice(12), b1.clone(), b2.clone()];
        buffers.iter().for_each(|x| x.claim(&pool));

        assert_eq!(pool.allocated(), b1.capacity() + b2.capacity());
        drop(buffers);
        assert_eq!(pool.allocated(), b1.capacity() + b2.capacity());
        drop(b2);
        assert_eq!(pool.allocated(), b1.capacity());
        drop(b1);
        assert_eq!(pool.allocated(), 0);
    }
}
