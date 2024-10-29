use std::sync::{Arc, Barrier, Mutex, RwLock};
use std::sync::atomic::{AtomicI32, Ordering};
// use criterion::black_box;
use criterion::{criterion_group, criterion_main, Criterion};


#[allow(unused)]
fn benchmark_lock_mutex(c: &mut Criterion) {

    c.bench_function("MUTEX - 3", move |b| b.iter( || {
        let num = 3;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(Mutex::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.lock().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.lock().unwrap();
        assert_eq!(res, num);
    } ));

    c.bench_function("MUTEX - 10", move |b| b.iter( || {
        let num = 10;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(Mutex::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.lock().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.lock().unwrap();
        assert_eq!(res, num);
    } ));

    c.bench_function("MUTEX - 100", move |b| b.iter( || {
        let num = 100;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(Mutex::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.lock().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.lock().unwrap();
        assert_eq!(res, num);
    } ));

    c.bench_function("MUTEX - 1000", move |b| b.iter( || {
        let num = 1000;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(Mutex::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.lock().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.lock().unwrap();
        assert_eq!(res, num);
    } ));

}


#[allow(unused)]
fn benchmark_lock_rwlock(c: &mut Criterion) {

    c.bench_function("RW_LOCK - 3", move |b| b.iter( || {
        let num = 3;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(RwLock::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.write().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.read().unwrap();
        assert_eq!(res, num);
    } ));

    c.bench_function("RW_LOCK - 10", move |b| b.iter( || {
        let num = 10;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(RwLock::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.write().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.read().unwrap();
        assert_eq!(res, num);
    } ));

    c.bench_function("RW_LOCK - 100", move |b| b.iter( || {
        let num = 100;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(RwLock::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.write().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.read().unwrap();
        assert_eq!(res, num);
    } ));

    c.bench_function("RW_LOCK - 1000", move |b| b.iter( || {
        let num = 1000;
        let barrier = Arc::new(Barrier::new(num));
        let mut threads = Vec::new();
        let locked_num = Arc::new(RwLock::new(1));

        for _ in 1..num {
            let b = barrier.clone();
            let num = locked_num.clone();
            let thread = std::thread::spawn(move|| {
                b.wait();
                *num.write().unwrap() += 1;
            });
            threads.push(thread);
        }

        barrier.wait();

        for t in threads {
            t.join().unwrap();
        }
        let res = *locked_num.read().unwrap();
        assert_eq!(res, num);
    } ));

}

fn benchmark_atomic(c: &mut Criterion) {

    c.bench_function("mutex - 1_000_000", move |b| b.iter( || {
        let expected = 1_000_000;
        let num = Arc::new(Mutex::new(0));
        let thread_count = usize::from(std::thread::available_parallelism().unwrap());
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut threads = Vec::with_capacity(thread_count);

        for _ in 0..thread_count {
            let barrier_clone = barrier.clone();
            let num_clone = num.clone();

            let t = std::thread::spawn(move || {
                barrier_clone.wait();
                while *(num_clone.lock().unwrap()) < expected {
                    let mut guard = num_clone.lock().unwrap();
                    let cur = *guard;
                    if cur < expected {
                        *guard += 1;
                    }
                }
            });
            threads.push(t)
        }

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(expected, *num.lock().unwrap());
        // println!("num: {}", *num.lock().unwrap());
    } ));

    c.bench_function("atomic - 1_000_000", move |b| b.iter( || {
        let expected = 1_000_000;
        let num = Arc::new(AtomicI32::new(0));
        let thread_count = usize::from(std::thread::available_parallelism().unwrap());
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut threads = Vec::with_capacity(thread_count);

        for _ in 0..thread_count {
            let barrier_clone = barrier.clone();
            let num_clone = num.clone();

            let t = std::thread::spawn(move || {
                barrier_clone.wait();

                while num_clone.load(Ordering::Acquire) < expected {
                    let cur = num_clone.load(Ordering::Acquire);
                    let new = cur + 1;
                    if new <= expected {
                        let _ = num_clone.compare_exchange(cur, new, Ordering::AcqRel, Ordering::Relaxed);
                    }
                }
            });
            threads.push(t)
        }

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(expected, num.load(Ordering::Acquire));
    } ));

}

fn benchmark_atomic_only_increment(c: &mut Criterion) {

    c.bench_function("mutex increment - 1_000_000", move |b| b.iter( || {
        let expected = 1_000_000;
        let num = Arc::new(Mutex::new(0));
        let thread_count = usize::from(std::thread::available_parallelism().unwrap());
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut threads = Vec::with_capacity(thread_count);

        for _ in 0..thread_count {
            let barrier_clone = barrier.clone();
            let num_clone = num.clone();

            let t = std::thread::spawn(move || {
                barrier_clone.wait();

                for _ in 0..expected {
                    *num_clone.lock().unwrap() += 1;
                }

            });
            threads.push(t)
        }

        for t in threads {
            t.join().unwrap();
        }

        let actual = *num.lock().unwrap() as usize;
        assert_eq!(expected * thread_count, actual);
        // println!("num: {}", actual);
    } ));

    c.bench_function("atomic Aq/Rel increment - 1_000_000", move |b| b.iter( || {
        let expected = 1_000_000;
        let num = Arc::new(AtomicI32::new(0));
        let thread_count = usize::from(std::thread::available_parallelism().unwrap());
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut threads = Vec::with_capacity(thread_count);

        for _ in 0..thread_count {
            let barrier_clone = barrier.clone();
            let num_clone = num.clone();

            let t = std::thread::spawn(move || {
                barrier_clone.wait();

                for _ in 0..expected {
                    // num_clone.fetch_add(1,Ordering::AcqRel);
                    num_clone.fetch_add(1,Ordering::AcqRel);
                }

            });
            threads.push(t)
        }

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(expected * thread_count, num.load(Ordering::Acquire) as usize);
        // println!("num: {}", num.load(Ordering::Acquire));
    } ));

    c.bench_function("relaxed atomic - 1_000_000", move |b| b.iter( || {
        let expected = 1_000_000;
        let num = Arc::new(AtomicI32::new(0));
        let thread_count = usize::from(std::thread::available_parallelism().unwrap());
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut threads = Vec::with_capacity(thread_count);

        for _ in 0..thread_count {
            let barrier_clone = barrier.clone();
            let num_clone = num.clone();

            let t = std::thread::spawn(move || {
                barrier_clone.wait();

                for _ in 0..expected {
                    num_clone.fetch_add(1,Ordering::Relaxed);
                }

            });
            threads.push(t)
        }

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(expected * thread_count, num.load(Ordering::Relaxed) as usize);
        // println!("num: {}", num.load(Ordering::Acquire));
    } ));

    c.bench_function(" SeqCst atomic - 1_000_000", move |b| b.iter( || {
        let expected = 1_000_000;
        let num = Arc::new(AtomicI32::new(0));
        let thread_count = usize::from(std::thread::available_parallelism().unwrap());
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut threads = Vec::with_capacity(thread_count);

        for _ in 0..thread_count {
            let barrier_clone = barrier.clone();
            let num_clone = num.clone();

            let t = std::thread::spawn(move || {
                barrier_clone.wait();

                for _ in 0..expected {
                    // num_clone.fetch_add(1,Ordering::AcqRel);
                    num_clone.fetch_add(1,Ordering::SeqCst);
                }

            });
            threads.push(t)
        }

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(expected * thread_count, num.load(Ordering::SeqCst) as usize);
        // println!("num: {}", num.load(Ordering::Acquire));
    } ));

}

// criterion_group!(benches, benchmark_lock_mutex, benchmark_lock_rwlock);
// criterion_group!(benches, benchmark_atomic);
// criterion_group!(benches, benchmark_atomic_only_increment);
criterion_group!(benches, benchmark_lock_mutex, benchmark_lock_rwlock, benchmark_atomic, benchmark_atomic_only_increment);
// criterion_group!(benches, benchmark_range);
criterion_main!(benches);