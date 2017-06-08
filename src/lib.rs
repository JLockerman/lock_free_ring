#![feature(attr_literals)]
#![feature(core_intrinsics)]
#![feature(i128_type)]
#![feature(untagged_unions)]
#![feature(repr_align)]

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
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
            DataCell{ unpacked: EpochPtr::null_at(0) })
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
            for i in 0..len {
                let writes = writes + i;
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
            }
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
            for i in 0..len {
                let reads = reads + i;
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
            }
            let new_reads = self.read_start.load(Ordering::Relaxed);
            if new_reads == reads {
                return None
            }
            reads = new_reads
        }
    }
}

pub unsafe trait PtrSized: Sized {}

unsafe impl<T> PtrSized for Box<T> {}
unsafe impl<T> PtrSized for Option<Box<T>> {}
unsafe impl<T> PtrSized for *mut T {}
unsafe impl<T> PtrSized for *const T {}
unsafe impl<'a, T> PtrSized for &'a mut T {}
unsafe impl<'a, T> PtrSized for &'a T {}

union DataCell<T> {
    unpacked: EpochPtr,
    packed: AtomicU128,
    phantom: PhantomData<T>,
}

impl<T> DataCell<T> {
    fn take_at_epoch(&self, load_epoch: usize) -> Result<Option<T>, bool>
    where T: PtrSized {
        unsafe {
            let cell_val = self.unpacked.load();
            let is_empty = cell_val.ptr.load(Ordering::Relaxed) == 0;
            let cell_val = cell_val.to_u128();
            let next_epoch = if is_empty { load_epoch } else { load_epoch + 1 };
            let replacement = EpochPtr::null_at(next_epoch).to_u128();
            let res = self.packed.compare_exchange(cell_val, replacement);
            match res {
                Ok(..) if is_empty => { Ok(None) },
                Ok(packed) => {
                    let (_, t): (usize, usize) = mem::transmute(packed);
                    if t == 0 { return Ok(None) }
                    let t: T = mem::transmute_copy(&t);
                    Ok(Some(t))
                },
                Err(curr_val) => {
                    let (mut epoch, val): (usize, usize) = mem::transmute(curr_val);
                    if epoch == load_epoch {
                        if val == 0 { return Ok(None) }
                        let res = self.packed.compare_exchange(curr_val, replacement);
                        match res {
                            Ok(packed) => {
                                let (_, t): (usize, usize) = mem::transmute(packed);
                                if t == 0 { return Ok(None) }
                                let t: T = mem::transmute_copy(&t);
                                return Ok(Some(t))
                            },
                            Err(packed) => {
                                let (e, _): (usize, usize) = mem::transmute(packed);
                                epoch = e
                            }
                        }
                    }
                    Err(epoch > load_epoch)
                }
            }
        }
    }

    fn put_at_epoch(&self, store_epoch: usize, data: T) -> Option<(T, bool)> {
        unsafe {
            let cell_val = EpochPtr::null_at(store_epoch);
            let cell_val = cell_val.to_u128();
            let data = {
                let u = mem::transmute_copy(&data);
                mem::forget(data);
                u
            };
            let replacement = EpochPtr::val(store_epoch, data);
            let replacement = replacement.to_u128();
            let res = self.packed.compare_exchange(cell_val, replacement);
            match res {
                Ok(..) => None,
                Err(packed) => {
                    let (epoch, _): (usize, usize) = mem::transmute_copy(&packed);
                    Some((mem::transmute_copy(&data), epoch > store_epoch))
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
struct EpochPtr {
    epoch: AtomicUsize,
    ptr: AtomicUsize,
}

impl EpochPtr {
    fn load(&self) -> Self {
        let epoch = AtomicUsize::new(self.epoch.load(Ordering::Relaxed));
        let ptr = AtomicUsize::new(self.ptr.load(Ordering::Relaxed));
        EpochPtr{epoch, ptr}
    }

    fn to_u128(self) -> u128 {
        unsafe { mem::transmute(self) }
    }

    fn null_at(epoch: usize) -> Self {
        EpochPtr{epoch: AtomicUsize::new(epoch), ptr: AtomicUsize::new(0)}
    }

    fn val(epoch: usize, val: usize) -> Self {
        EpochPtr{epoch: AtomicUsize::new(epoch), ptr: AtomicUsize::new(val)}
    }
}

#[repr(align(16))]
struct AtomicU128 {
    data: UnsafeCell<u128>,
}

impl AtomicU128 {
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
        assert_eq!(mem::size_of::<EpochPtr>(), 16);
        assert_eq!(mem::size_of::<DataCell<*mut u8>>(), 16);
    }
}
