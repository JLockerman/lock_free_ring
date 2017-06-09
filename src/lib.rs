#![feature(attr_literals)]
#![feature(core_intrinsics)]
#![feature(hint_core_should_pause)]
#![feature(i128_type)]
#![feature(untagged_unions)]
#![feature(repr_align)]

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::{mem, ptr};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering, hint_core_should_pause};
use std::sync::Arc;

pub struct Ring<T> {
    data: Box<[DataCell<T>]>,
    //TODO pad
    read_start: CacheAligned<AtomicUsize>,
    write_start: CacheAligned<AtomicUsize>,
}

pub fn channel<T>(size: usize) -> (Arc<Ring<T>>, Arc<Ring<T>>) {
    let ring = Arc::new(Ring::new(size));
    (ring.clone(), ring)
}

unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    pub fn new(cap: usize) -> Self {
        let data: Vec<_> = (0..cap).map(|_|
            DataCell{ unpacked: AtomicEpochPtr::null_at(0) })
            .collect();
        let data = data.into_boxed_slice();
        Ring {
            data: data,
            read_start: Default::default(),
            write_start: Default::default(),
        }
    }
}

impl<T> Ring<T>
where T: PtrSized {

    pub fn try_push(&self, mut data: T) -> Result<(), T> {
        let len = self.data.len();
        let mut writes = self.write_start.load(Ordering::Relaxed);
        'push: loop {
            let epoch = writes / (len as u64) as usize;
            let loc = writes % (len as u64) as usize;
            let reput = self.data[loc].put_at_epoch(epoch, data);
            match reput {
                None => {
                    self.write_start.fetch_add(1, Ordering::Relaxed);
                    return Ok(())
                },
                Some((d, keep_going)) => {
                    if !keep_going {
                        return Err(d)
                    }
                    data = d;
                }
            }
            hint_core_should_pause();
            let new_writes = self.write_start.load(Ordering::Relaxed);
            if new_writes == writes {
                return Err(data)
            }
            writes = new_writes
        }
    }

    pub fn try_pop(&self) -> Option<T> {
        let len = self.data.len();
        let mut reads = self.read_start.load(Ordering::Relaxed);
        'pop: loop {
            let epoch = reads / (len as u64) as usize;
            let loc = reads % (len as u64) as usize;
            let reget = self.data[loc].take_at_epoch(epoch);
            match reget {
                Ok(None) => {
                    return None
                },
                Ok(Some(data)) => {
                    self.read_start.fetch_add(1, Ordering::Relaxed);
                    return Some(data)
                },
                Err(false) => {
                    return None
                }
                Err(true) => {}
            }
            hint_core_should_pause();
            let new_reads = self.read_start.load(Ordering::Relaxed);
            if new_reads == reads {
                return None
            }
            reads = new_reads
        }
    }
}

impl<T> fmt::Debug for Ring<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct(&"Ring")
            .field("read_start", &self.read_start)
            .field("write_start", &self.write_start)
            .field("data", &self.data)
            .finish()
    }
}

pub unsafe trait PtrSized: Sized {
    fn to_ptr(self) -> *mut () {
        unsafe {
            let ptr = mem::transmute_copy(&self);
            mem::forget(self);
            ptr
        }
    }

    unsafe fn from_ptr(ptr: *mut ()) -> Self {
        mem::transmute_copy(&ptr)
    }
}

unsafe impl<T> PtrSized for Box<T> {}
unsafe impl<T> PtrSized for Option<Box<T>> {}
unsafe impl<T> PtrSized for *mut T {}
unsafe impl<T> PtrSized for *const T {}
unsafe impl<'a, T> PtrSized for &'a mut T {}
unsafe impl<'a, T> PtrSized for &'a T {}

union DataCell<T> {
    unpacked: AtomicEpochPtr,
    packed: AtomicU128,
    phantom: PhantomData<T>,
}

impl<T> DataCell<T> {
    fn take_at_epoch(&self, load_epoch: usize) -> Result<Option<T>, bool>
    where T: PtrSized {
        unsafe {
            let nop = EpochPtr::null_at(load_epoch).to_u128();
            let empty = EpochPtr::null_at(load_epoch).to_u128();
            let res = self.packed.compare_exchange(empty, nop);
            match res {
                Ok(..) => Ok(None),
                Err(curr_val) => {
                    let EpochPtr{mut epoch, ptr} = EpochPtr::from_u128(curr_val);
                    if epoch == load_epoch {
                        if ptr.is_null() { return Ok(None) }
                        let replacement = EpochPtr::null_at(load_epoch + 1).to_u128();
                        let res = self.packed.compare_exchange(curr_val, replacement);
                        match res {
                            Ok(packed) => {
                                let ptr = EpochPtr::from_u128(packed).ptr;
                                if ptr.is_null() { return Ok(None) }
                                return Ok(Some(PtrSized::from_ptr(ptr)))
                            },
                            Err(packed) => {
                                epoch = EpochPtr::from_u128(packed).epoch
                            }
                        }
                    }
                    Err(epoch > load_epoch)
                },
            }
        }
    }

    fn put_at_epoch(&self, store_epoch: usize, data: T) -> Option<(T, bool)>
    where T: PtrSized {
        unsafe {
            let cell_val = EpochPtr::null_at(store_epoch);
            let cell_val = cell_val.to_u128();
            let data = data.to_ptr();
            let replacement = EpochPtr::val(store_epoch, data);
            let replacement = replacement.to_u128();
            let res = self.packed.compare_exchange(cell_val, replacement);
            match res {
                Ok(..) => None,
                Err(packed) => {
                    let epoch = EpochPtr::from_u128(packed).epoch;
                    Some((PtrSized::from_ptr(data), epoch > store_epoch))
                }
            }
        }
    }
}

impl<T> fmt::Debug for DataCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        unsafe { f.debug_tuple(&"DataCell").field(&self.unpacked).finish() }
    }
}

#[derive(Debug)]
#[repr(C)]
struct AtomicEpochPtr {
    epoch: AtomicUsize,
    ptr: AtomicPtr<()>,
}

#[derive(Debug)]
#[repr(C)]
struct EpochPtr {
    epoch: usize,
    ptr: *mut (),
}

impl AtomicEpochPtr {
    fn null_at(epoch: usize) -> Self {
        AtomicEpochPtr{epoch: AtomicUsize::new(epoch), ptr: AtomicPtr::new(ptr::null_mut())}
    }
}

impl EpochPtr {
    fn to_u128(self) -> u128 {
        unsafe { mem::transmute(self) }
    }

    fn from_u128(packed: u128) -> Self {
        unsafe { mem::transmute(packed) }
    }

    fn null_at(epoch: usize) -> Self {
        EpochPtr{epoch, ptr: ptr::null_mut()}
    }

    fn val(epoch: usize, ptr: *mut ()) -> Self {
        EpochPtr{epoch, ptr}
    }
}

#[repr(align(16))]
struct AtomicU128 {
    data: UnsafeCell<u128>,
}

impl AtomicU128 {
    #[inline]
    fn compare_exchange(&self, current: u128, new: u128) -> Result<u128, u128> {
        use std::intrinsics::atomic_cxchg;
        let (val, ok) = unsafe { atomic_cxchg(self.data.get(), current, new) };
        if ok { Ok(val) } else { Err(val) }
    }
}

#[derive(Default, Debug)]
#[repr(align(16))]
struct CacheAligned<T>(T);

impl<T> std::ops::Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::mem;

    #[test]
    fn align() {
        assert_eq!(mem::align_of::<AtomicU128>(), 16);
        assert_eq!(mem::align_of::<DataCell<*mut u8>>(), 16);
    }

    #[test]
    fn size() {
        assert_eq!(mem::size_of::<AtomicU128>(), 16);
        assert_eq!(mem::size_of::<AtomicUsize>(), 8);
        assert_eq!(mem::size_of::<AtomicEpochPtr>(), 16);
        assert_eq!(mem::size_of::<DataCell<*mut u8>>(), 16);
    }
}
