use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::File;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct TrackingAllocator {
    pub bytes_allocated: AtomicUsize,
}

impl TrackingAllocator {
    pub const fn new() -> Self {
        Self {
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    pub fn bytes_allocated(&self) -> usize {
        self.bytes_allocated.load(Ordering::Relaxed)
    }
}

impl Default for TrackingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.bytes_allocated
            .fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.bytes_allocated
            .fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

#[test]
fn test_metadata_heap_memory() {
    let test_data = arrow::util::test_util::parquet_test_data();
    let path = format!("{test_data}/alltypes_dictionary.parquet");
    let input_file = File::open(path).unwrap();

    let baseline = ALLOCATOR.bytes_allocated();

    let options = ArrowReaderOptions::default();
    let reader_metadata = ArrowReaderMetadata::load(&input_file, options).unwrap();
    let metadata = Arc::clone(reader_metadata.metadata());
    drop(reader_metadata);

    let metadata_heap_size = metadata.memory_size();

    let allocated = ALLOCATOR.bytes_allocated() - baseline;
    black_box(metadata);
    let arc_overhead = std::mem::size_of::<usize>() * 2;

    assert!(metadata_heap_size > 0);
    assert_eq!(metadata_heap_size + arc_overhead, allocated);
}
