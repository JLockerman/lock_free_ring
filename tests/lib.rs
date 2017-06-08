extern crate lock_free_ring;

mod from_deque {
    //tests from kinghajj/deque
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use lock_free_ring::*;

    use std::thread;

    #[test]
    fn smoke() {
        let (s, r) = channel(1);

        assert_eq!(r.try_pop(), None);
        assert_eq!(r.clone().try_pop(), None);
        assert_eq!(s.try_push(&1), Ok(()));
        assert_eq!(r.try_pop(), Some(&1));
        assert_eq!(s.try_push(&3), Ok(()));
        assert_eq!(s.try_push(&1), Err(&1));
        assert_eq!(r.try_pop(), Some(&3));
        assert_eq!(s.try_push(&4), Ok(()));
        assert_eq!(r.clone().try_pop(), Some(&4));
        assert_eq!(s.clone().try_push(&2), Ok(()));
        assert_eq!(r.try_pop(), Some(&2));
        assert_eq!(s.clone().try_push(&22), Ok(()));
        assert_eq!(r.clone().try_pop(), Some(&22));
        assert_eq!(r.try_pop(), None);
        assert_eq!(r.clone().try_pop(), None);
    }

    #[test]
    fn stealpush() {
        struct NotZero(usize);
        unsafe impl PtrSized for NotZero {}

        static AMT: usize = 100000;
        let (s, r) = channel((AMT / 2).next_power_of_two());
        let t = thread::spawn(move || {
            let mut left = AMT;
            while left > 0 {
                match r.try_pop() {
                    Some(NotZero(i)) => {
                        assert_eq!(i as usize - 1, AMT - left);
                        left -= 1;
                    }
                    None => { thread::yield_now() }
                }
            }
        });

        for i in 0..AMT {
            while let Err(..) = s.try_push(NotZero(i+1)) {
                thread::yield_now()
            }
        }

        t.join().unwrap();
    }

    fn stampede(
        s: Arc<Ring<Box<usize>>>, r: Arc<Ring<Box<usize>>>, nthreads: isize, amt: usize
    ) {
        for _ in 0..amt {
            while let Err(..) = s.try_push(Box::new(20)) {
                thread::yield_now()
            }
        }
        static HANDLED: AtomicUsize = ATOMIC_USIZE_INIT;

        let threads = (0..nthreads).map(|_| {
            let r = r.clone();
            thread::spawn(move || {
                while HANDLED.load(SeqCst) < amt {
                    match r.try_pop() {
                        Some(ref i) if **i == 20 => {
                            HANDLED.fetch_add(1, SeqCst);
                        }
                        Some(i) => panic!("invalid val {:?}", i),
                        None => { thread::yield_now() }
                    }
                }
            })
        }).collect::<Vec<_>>();

        for thread in threads.into_iter() {
            thread.join().unwrap();
        }
    }

    #[test]
    fn run_stampede() {
        let (s, r) = channel(10000);
        stampede(s, r, 8, 10000);
    }

    #[test]
    fn many_stampede() {
        static AMT: usize = 4;
        let threads = (0..AMT).map(|_| {
            let (s, r) = channel(10000);
            thread::spawn(move || {
                stampede(s, r, 4, 10000);
            })
        }).collect::<Vec<_>>();

        for thread in threads.into_iter() {
            thread.join().unwrap();
        }
    }
}// end from_deque
