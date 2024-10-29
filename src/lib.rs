use crate::key::{HasMax, SearchKey};
use std::borrow::BorrowMut;
use std::cell::Cell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::collections::Bound::{Excluded, Included};
use std::fmt::Debug;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

mod id_thread_locals;
pub mod key;
mod named_thread_locals;
pub mod thread_events;

pub struct RangeLock<T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T>> {
    rw_lock: RwLock<BTreeMap<SearchKey<T>, Mutex<Cell<Instant>>>>,
}

/**
* SAFETY:
* safe to share across thread as btree map changes are protected by RwLock.
* Cell<Instant> is protected by Mutex
*/
unsafe impl<T> Send for RangeLock<T> where
    T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T> + 'static
{
}

unsafe impl<T> Sync for RangeLock<T> where
    T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T> + 'static
{
}

#[allow(unused)]
impl<T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T> + Debug> RangeLock<T> {
    pub fn new() -> Self {
        Self {
            rw_lock: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn keys_recursive(&self, start_key: SearchKey<T>) -> Vec<SearchKey<T>> {
        let mut key_end = start_key.clone();
        key_end.append_max();

        let mut keys = vec![];

        for (key, _) in self
            .rw_lock
            .read()
            .unwrap()
            .range((Included(start_key), Excluded(key_end)))
        {
            keys.push(key.clone());
        }
        keys
    }

    pub fn locked(&self, key: SearchKey<T>) -> bool {
        let read_guard = self.rw_lock.read().unwrap();
        for k in key.prefix_keys_inclusive() {
            if let Some(m) = read_guard.get(&k) {
                if m.try_lock().is_err() {
                    return true;
                }
            }
        }
        false
    }

    pub fn key_exists(&self, key: &SearchKey<T>) -> bool {
        self.rw_lock.read().unwrap().get(key).is_some()
    }

    pub fn insert_lock(&self, key: SearchKey<T>) {
        let key = key.clone(); // todo: refactor
        let mut write_guard = self.rw_lock.write().unwrap();
        if let Entry::Vacant(vacant) = write_guard.entry(key) {
            vacant.insert(Mutex::new(Cell::new(Instant::now())));
        }
    }

    pub fn delete_lock(&self, key: &SearchKey<T>) -> Option<(SearchKey<T>, Mutex<Cell<Instant>>)> {
        let mut range_lock = self.rw_lock.write().unwrap();
        range_lock.remove_entry(key)
    }

    pub fn delete_lock_recursive(&self, key: &SearchKey<T>) {
        let mut range_lock = self.rw_lock.write().unwrap();

        let mut key_end = key.clone();
        key_end.append_max();

        let mut keys = vec![];
        for (key, _) in range_lock.range((Included(key.clone()), Included(key_end))) {
            keys.push(key.clone());
        }

        for each_key in keys {
            range_lock.borrow_mut().remove_entry(&each_key);
        }
    }

    pub fn with_locked(
        &self,
        key: SearchKey<T>,
        callback: impl FnOnce(Vec<MutexGuard<Cell<Instant>>>),
    ) {
        loop {
            let global_read = self.rw_lock.read().unwrap();
            if global_read.get(&key).is_none() {
                drop(global_read);
                self.insert_lock(key.clone());
                continue;
            }
            let mut locked = Vec::new();
            let cur_thread = std::thread::current();
            let cur_thread_name = cur_thread.name().unwrap_or("<no_name>");
            for k in key.prefix_keys_inclusive() {
                if let Some(m) = global_read.get(&k) {
                    if m.try_lock().is_err() || k == key {
                        let mut m_guard = m.lock().unwrap();
                        m_guard.deref_mut().replace(Instant::now());
                        locked.push(m_guard); // keep locked
                    }
                }
            }
            callback(locked);
            return;
        }
    }

    pub fn with_locked_recursive(
        &self,
        key: SearchKey<T>,
        callback: impl FnOnce(Vec<MutexGuard<Cell<Instant>>>),
    ) {
        loop {
            let global_read = self.rw_lock.read().unwrap();
            if global_read.get(&key).is_none() {
                drop(global_read);
                self.insert_lock(key.clone());
                continue;
            }
            let mut locked = Vec::new();
            for k in key.prefix_keys_inclusive() {
                if let Some(m) = global_read.get(&k) {
                    let mut m_guard = m.lock().unwrap();
                    m_guard.deref_mut().replace(Instant::now());
                    locked.push(m_guard); // keep locked
                }
            }

            let mut key_end = key.clone();
            key_end.append_max();

            let range_locks = global_read.range((Excluded(key), Excluded(key_end)));
            for (key, lock) in range_locks {
                let mut m_guard = lock.lock().unwrap();
                m_guard.deref_mut().replace(Instant::now());
                locked.push(m_guard);
            }
            callback(locked);
            return;
        }
    }

    pub fn with_locked_recursive_and_nonrecursive(
        &self,
        key: SearchKey<T>,
        recursive_key: SearchKey<T>,
        callback: impl FnOnce(Vec<MutexGuard<Cell<Instant>>>),
    ) {
        let global_read = self.rw_lock.read().unwrap();
        if global_read.get(&key).is_none() {
            drop(global_read);
            self.insert_lock(key.clone());
            return self.with_locked_recursive_and_nonrecursive(key, recursive_key, callback);
        }
        if global_read.get(&recursive_key).is_none() {
            drop(global_read);
            self.insert_lock(recursive_key.clone());
            return self.with_locked_recursive_and_nonrecursive(key, recursive_key, callback);
        }
        let mut common_key_until_idx = key.common_prefix_with(&recursive_key);

        let mut locks: Vec<MutexGuard<Cell<Instant>>> = Vec::new();
        let keys = key.prefix_keys_inclusive();
        for i in 0..common_key_until_idx {
            if let Some(cur_key) = keys.get(i) {
                if let Some(m_lock) = global_read.get(cur_key) {
                    if m_lock.try_lock().is_err() {
                        let mut m_guard = m_lock.lock().unwrap();
                        m_guard.replace(Instant::now());
                        locks.push(m_guard);
                    }
                }
            }
        }
        let mut exclude_keys = BTreeSet::new();
        for i in common_key_until_idx..keys.len() {
            if let Some(cur_key) = keys.get(i) {
                if let Some(m_lock) = global_read.get(cur_key) {
                    if m_lock.try_lock().is_err() {
                        let mut m_guard = m_lock.lock().unwrap();
                        m_guard.replace(Instant::now());
                        locks.push(m_guard);
                        exclude_keys.insert(cur_key);
                    }
                }
            }
        }
        
        let keys = recursive_key.prefix_keys_inclusive();
        for i in common_key_until_idx..keys.len() {
            if let Some(cur_key) = keys.get(i) {
                if let Some(m_lock) = global_read.get(cur_key) {
                    if m_lock.try_lock().is_err() {
                        let mut m_guard = m_lock.lock().unwrap();
                        m_guard.replace(Instant::now());
                        locks.push(m_guard);
                    }
                }
            }
        }
        let mut key_end = recursive_key.clone();
        key_end.append_max();

        let range_locks = global_read.range((Excluded(recursive_key), Excluded(key_end)));
        for (key, lock) in range_locks {
            if lock.try_lock().is_err() && !exclude_keys.contains(key){
                let mut m_guard = lock.lock().unwrap();
                m_guard.replace(Instant::now());
                locks.push(m_guard);
            }
        }
        
        callback(locks);
    }

    fn start_delete_expired_keys(&self, expire_duration: Duration, check_frequency: Duration) {
        loop {
            std::thread::sleep(check_frequency);
            let mut write_lock = self.rw_lock.write().unwrap();

            let mut keys = vec![];

            for (key, lock) in write_lock.iter() {
                if let Ok(inst) = lock.try_lock() {
                    if inst.get().elapsed() > expire_duration {
                        keys.push(key.clone());
                    }
                }
            }

            for key in keys {
                write_lock.borrow_mut().remove_entry(&key);
            }
        }
    }
}

impl<T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T> + Debug> Default for RangeLock<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
impl<T: Ord + PartialOrd + Eq + PartialEq + Clone + HasMax<T> + Debug + 'static> RangeLock<T> {
    pub fn start_delete_expired_keys_async(
        this: Arc<RangeLock<T>>,
        expire_duration: Duration,
        check_frequency: Duration,
    ) {
        let arc_2 = this.clone();
        std::thread::spawn(move || {
            arc_2.start_delete_expired_keys(expire_duration, check_frequency);
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::id_thread_locals::IdThreadLocals;
    use crate::named_thread_locals::FixedNamedThreadLocals;
    use crate::thread_events::CurThreadEvent;

    use super::*;
    use std::path::Path;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;
    use thread_events::ThreadLocals;

    #[test]
    fn test_recursive_range() {
        let tree: RangeLock<&str> = RangeLock::new();

        let path = Path::new("/home/user1/music");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let path = Path::new("/home/user1/photo");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let path = Path::new("/home/user2/photo");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let path = Path::new("/home/user3/");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let path = Path::new("/etc");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let path = Path::new("/home/user1/photo/2002");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let path = Path::new("/home/user1/zzzzz");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let path = Path::new("/");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        tree.insert_lock(sk);

        let start_key: SearchKey<_> = Path::new("/home/user1").try_into().unwrap();
        let inner = tree.keys_recursive(start_key);
        let mut actual = Vec::new();
        for each in inner {
            actual.push(each);
        }
        let expected: Vec<SearchKey<&str>> = vec![
            Path::new("/home/user1/music").try_into().unwrap(),
            Path::new("/home/user1/photo/").try_into().unwrap(),
            Path::new("/home/user1/photo/2002").try_into().unwrap(),
            Path::new("/home/user1/zzzzz").try_into().unwrap(),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_prefix_keys() {
        let path = Path::new("/");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        let expected_inclusive = vec![SearchKey::from_val("/")];
        assert_eq!(sk.prefix_keys_inclusive(), expected_inclusive);
        assert_eq!(sk.prefix_keys_exclusive(), vec![]);

        let path = Path::new("/home/users/emix");
        let sk: SearchKey<&str> = path.try_into().unwrap();
        let mut expected = vec![
            SearchKey::from_val("/"),
            SearchKey::from_vec(vec!["/", "home"]),
            SearchKey::from_vec(vec!["/", "home", "users"]),
            SearchKey::from_vec(vec!["/", "home", "users", "emix"]),
        ];
        assert_eq!(sk.prefix_keys_inclusive(), expected);
        expected.pop();
        assert_eq!(sk.prefix_keys_exclusive(), expected);
    }

    #[test]
    fn test_locking_root_simultaneously() {
        let range = Arc::new(RangeLock::new());
        let range_t1 = range.clone();
        let root_key: SearchKey<&str> = Path::new("/").try_into().unwrap();
        let root_key_clone = root_key.clone();
        let barrier = Arc::new(Barrier::new(2));
        let barrier_t1 = barrier.clone();
        let thread_locals = Arc::new(IdThreadLocals::new());
        let thread_locals_t1 = Arc::clone(&thread_locals);

        let sleep_millis: u64 = 20;
        thread::spawn(move || {
            barrier_t1.wait();
            range_t1.with_locked(root_key_clone, |_locks| {
                thread_locals_t1.start();
                thread::sleep(Duration::from_millis(sleep_millis));
                thread_locals_t1.finish();
            });
        });

        barrier.wait();
        thread::sleep(Duration::from_millis(sleep_millis / 2));
        let is_locked = range.locked(root_key.clone());
        assert!(is_locked);

        range.with_locked(root_key, move |_locks| {
            thread_locals.start();
            thread::sleep(Duration::from_millis(sleep_millis));
            thread_locals.finish();
            let mut events = thread_locals.events();
            assert!(!CurThreadEvent::contains_any_overlap(events.as_mut()));
        });
    }

    #[test]
    fn test_locking_independent_keys_simultaneously() {
        let range = Arc::new(RangeLock::new());
        let range_t1 = range.clone();
        let barrier = Arc::new(Barrier::new(2));
        let barrier_t1 = barrier.clone();
        let thread_locals = std::sync::Arc::new(IdThreadLocals::new());
        let thread_locals_t1 = Arc::clone(&thread_locals);
        let user_1: SearchKey<&str> = Path::new("/home/user_1").try_into().unwrap();
        let user_1_t1 = user_1.clone();
        let user_2: SearchKey<&str> = Path::new("/home/user_2").try_into().unwrap();
        range.insert_lock(user_1.clone());
        range.insert_lock(user_2.clone());

        let sleep_millis: u64 = 20;
        thread::spawn(move || {
            barrier_t1.wait();
            range_t1.with_locked(user_1_t1, |_locks| {
                thread_locals_t1.start();
                thread::sleep(Duration::from_millis(sleep_millis));
                thread_locals_t1.finish();
            })
        });
        barrier.wait();
        thread::sleep(Duration::from_millis(sleep_millis / 2));
        let is_locked = range.locked(user_1);
        assert!(is_locked);

        range.with_locked(user_2, move |_locks| {
            thread_locals.start();
            thread::sleep(Duration::from_millis(sleep_millis));
            thread_locals.finish();
            let mut events = thread_locals.events();
            assert!(CurThreadEvent::contains_any_overlap(events.as_mut()));
        });
    }

    #[test]
    fn test_locking_multilevel() {
        let range = Arc::new(RangeLock::new());
        let range_t1 = range.clone();
        let range_t2 = range.clone();

        let etc_sk: SearchKey<&str> = Path::new("/etc").try_into().unwrap();
        range.insert_lock(etc_sk.clone());
        let home_sk: SearchKey<&str> = Path::new("/home/").try_into().unwrap();
        range.insert_lock(home_sk.clone());
        let user_1_sk: SearchKey<&str> = Path::new("/home/user1").try_into().unwrap();
        range.insert_lock(user_1_sk.clone());
        let user2_sk: SearchKey<&str> = Path::new("/home/user2").try_into().unwrap();
        range.insert_lock(user2_sk.clone());

        let thread_name_home = "thread_home";
        let thread_name_user1 = "thread_name_user1";
        let thread_name_etc = "thread_name_etc";

        let named_thead_locals = FixedNamedThreadLocals::new_arced(vec![
            thread_name_home.into(),
            thread_name_user1.into(),
            thread_name_etc.into(),
        ]);

        let named_thread_locals_clone_1 = Arc::clone(&named_thead_locals);
        let named_thread_locals_clone_2 = Arc::clone(&named_thead_locals);
        let named_thread_locals_clone_3 = Arc::clone(&named_thead_locals);

        let barrier = Arc::new(Barrier::new(3));
        let barrier_t1 = barrier.clone();
        let barrier_t2 = barrier.clone();

        let sleep_millis: u64 = 40;
        let t_home_handler = thread::Builder::new()
            .name(thread_name_home.into())
            .spawn(move || {
                barrier_t1.wait();
                range_t1.with_locked(home_sk.clone(), |_locks| {
                    named_thread_locals_clone_1.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_1.finish();
                })
            })
            .unwrap();

        let t_user1_handler = thread::Builder::new()
            .name(thread_name_user1.into())
            .spawn(move || {
                barrier_t2.wait();
                thread::sleep(Duration::from_millis(sleep_millis / 2));
                assert!(range_t2.locked(user_1_sk.clone()));
                range_t2.with_locked(user_1_sk.clone(), |_locks| {
                    named_thread_locals_clone_2.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_2.finish();
                })
            })
            .unwrap();

        let t_etc_handler = thread::Builder::new()
            .name(thread_name_etc.into())
            .spawn(move || {
                barrier.wait();

                thread::sleep(Duration::from_millis(sleep_millis / 2));
                assert!(!range.locked(etc_sk.clone()));

                range.with_locked(etc_sk.clone(), |_locks| {
                    named_thread_locals_clone_3.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_3.finish();
                })
            })
            .unwrap();

        t_home_handler.join().unwrap();
        t_user1_handler.join().unwrap();
        t_etc_handler.join().unwrap();

        let events = FixedNamedThreadLocals::arc_events(named_thead_locals);
        let home_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_home)
            .expect("thread with this name should exist");
        let user1_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_user1)
            .expect("thread with this name should exist");
        let etc_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_etc)
            .expect("thread with this name should exist");

        assert_eq!(home_event.overlaps_with(user1_event), Some(false));
        assert_eq!(home_event.overlaps_with(etc_event), Some(true));
    }

    #[test]
    fn test_locking_lock_in_range_not_recursive() {
        let range = Arc::new(RangeLock::new());
        let range_t1 = range.clone();
        let range_t2 = range.clone();

        let etc_sk: SearchKey<&str> = Path::new("/etc").try_into().unwrap();
        range.insert_lock(etc_sk.clone());
        let home_sk: SearchKey<&str> = Path::new("/home/").try_into().unwrap();
        range.insert_lock(home_sk.clone());
        let user_1_sk: SearchKey<&str> = Path::new("/home/user1").try_into().unwrap();
        range.insert_lock(user_1_sk.clone());
        let user2_sk: SearchKey<&str> = Path::new("/home/user2").try_into().unwrap();
        range.insert_lock(user2_sk.clone());

        let thread_name_home = "thread_home";
        let thread_name_user1 = "thread_name_user1";
        let thread_name_etc = "thread_name_etc";

        let named_thread_locals = FixedNamedThreadLocals::new_arced(vec![
            thread_name_home.into(),
            thread_name_user1.into(),
            thread_name_etc.into(),
        ]);

        let named_thread_locals_clone_1 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_2 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_3 = Arc::clone(&named_thread_locals);

        let barrier = Arc::new(Barrier::new(3));
        let barrier_t1 = barrier.clone();
        let barrier_t2 = barrier.clone();

        let sleep_millis: u64 = 40;
        let t_home_handler = thread::Builder::new()
            .name(thread_name_home.into())
            .spawn(move || {
                barrier_t1.wait();
                thread::sleep(Duration::from_millis(sleep_millis / 2));
                range_t1.with_locked(home_sk.clone(), |_locks| {
                    named_thread_locals_clone_1.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_1.finish();
                })
            })
            .unwrap();

        let t_user1_handler = thread::Builder::new()
            .name(thread_name_user1.into())
            .spawn(move || {
                barrier_t2.wait();
                range_t2.with_locked(user_1_sk.clone(), |_locks| {
                    named_thread_locals_clone_2.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_2.finish();
                })
            })
            .unwrap();

        let t_etc_handler = thread::Builder::new()
            .name(thread_name_etc.into())
            .spawn(move || {
                barrier.wait();

                thread::sleep(Duration::from_millis(5));
                assert!(!range.locked(etc_sk.clone()));

                range.with_locked(etc_sk.clone(), |_locks| {
                    named_thread_locals_clone_3.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_3.finish();
                })
            })
            .unwrap();

        t_home_handler.join().unwrap();
        t_user1_handler.join().unwrap();
        t_etc_handler.join().unwrap();

        let events = FixedNamedThreadLocals::arc_events(named_thread_locals);
        let home_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_home)
            .expect("thread with this name should exist");
        let user1_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_user1)
            .expect("thread with this name should exist");
        let etc_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_etc)
            .expect("thread with this name should exist");

        assert_eq!(home_event.overlaps_with(user1_event), Some(true));
        assert_eq!(home_event.overlaps_with(etc_event), Some(true));
    }

    #[test]
    fn test_locking_lock_in_range_recursive() {
        let range = Arc::new(RangeLock::new());
        let range_t1 = range.clone();
        let range_t2 = range.clone();

        let etc_sk: SearchKey<&str> = Path::new("/etc").try_into().unwrap();
        range.insert_lock(etc_sk.clone());
        let home_sk: SearchKey<&str> = Path::new("/home/").try_into().unwrap();
        range.insert_lock(home_sk.clone());
        let user_1_sk: SearchKey<&str> = Path::new("/home/user1").try_into().unwrap();
        range.insert_lock(user_1_sk.clone());
        let user2_sk: SearchKey<&str> = Path::new("/home/user2").try_into().unwrap();
        range.insert_lock(user2_sk.clone());

        let thread_name_home = "thread_home";
        let thread_name_user1 = "thread_name_user1";
        let thread_name_etc = "thread_name_etc";

        let named_thread_locals = FixedNamedThreadLocals::new_arced(vec![
            thread_name_home.into(),
            thread_name_user1.into(),
            thread_name_etc.into(),
        ]);

        let named_thread_locals_clone_1 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_2 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_3 = Arc::clone(&named_thread_locals);

        let barrier = Arc::new(Barrier::new(3));
        let barrier_t1 = barrier.clone();
        let barrier_t2 = barrier.clone();

        let sleep_millis: u64 = 40;
        let t_home_handler = thread::Builder::new()
            .name(thread_name_home.into())
            .spawn(move || {
                barrier_t1.wait();
                thread::sleep(Duration::from_millis(sleep_millis / 2));
                range_t1.with_locked_recursive(home_sk.clone(), |_locks| {
                    named_thread_locals_clone_1.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_1.finish();
                })
            })
            .unwrap();

        let t_user1_handler = thread::Builder::new()
            .name(thread_name_user1.into())
            .spawn(move || {
                barrier_t2.wait();
                range_t2.with_locked(user_1_sk.clone(), |_locks| {
                    named_thread_locals_clone_2.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_2.finish();
                })
            })
            .unwrap();

        let t_etc_handler = thread::Builder::new()
            .name(thread_name_etc.into())
            .spawn(move || {
                barrier.wait();

                thread::sleep(Duration::from_millis(5));
                assert!(!range.locked(etc_sk.clone()));

                range.with_locked(etc_sk.clone(), |_locks| {
                    named_thread_locals_clone_3.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_3.finish();
                })
            })
            .unwrap();

        t_home_handler.join().unwrap();
        t_user1_handler.join().unwrap();
        t_etc_handler.join().unwrap();

        let events = FixedNamedThreadLocals::arc_events(named_thread_locals);
        let home_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_home)
            .expect("thread with this name should exist");
        let user1_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_user1)
            .expect("thread with this name should exist");
        let etc_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_etc)
            .expect("thread with this name should exist");

        assert_eq!(home_event.overlaps_with(user1_event), Some(false));
        assert_eq!(home_event.overlaps_with(etc_event), Some(true));
    }

    #[test]
    fn with_locked_recursive_and_nonrecursive() {
        let range = Arc::new(RangeLock::new());
        let range_t1 = range.clone();
        let range_t2 = range.clone();
        let range_t3 = range.clone();
        let range_t4 = range.clone();

        let etc_sk: SearchKey<&str> = Path::new("/etc").try_into().unwrap();
        range.insert_lock(etc_sk.clone());

        let user_1_photos_2021_sk: SearchKey<&str> =
            Path::new("/home/user1/photos/2021/").try_into().unwrap();
        range.insert_lock(user_1_photos_2021_sk.clone());

        let user_1_photos_2021_ny_sk: SearchKey<&str> =
            Path::new("/home/user1/photos/2021/NY/").try_into().unwrap();
        range.insert_lock(user_1_photos_2021_ny_sk.clone());

        let user_1_photos_2021_tx_sk: SearchKey<&str> =
            Path::new("/home/user1/photos/2021/TX/").try_into().unwrap();
        range.insert_lock(user_1_photos_2021_tx_sk.clone());

        let user2_sk: SearchKey<&str> = Path::new("/home/user2").try_into().unwrap();
        range.insert_lock(user2_sk.clone());

        let user2_videos_2022_sk: SearchKey<&str> =
            Path::new("/home/user2/videos/2022").try_into().unwrap();
        range.insert_lock(user2_videos_2022_sk.clone());

        let thread_name_user1_ny = "thread_user1_ny";
        let thread_name_user2_2022 = "thread_name_user2_2022";
        let thread_name_etc = "thread_name_etc";
        let thread_name_final = "thread_name_final";
        let thread_name_pair_recursive = "thread_name_pair_recursive";

        let named_thread_locals = FixedNamedThreadLocals::new_arced(vec![
            thread_name_user1_ny.to_owned(),
            thread_name_user2_2022.to_owned(),
            thread_name_etc.to_owned(),
            thread_name_final.to_owned(),
            thread_name_pair_recursive.to_owned(),
        ]);

        let named_thread_locals_clone_1 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_2 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_3 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_4 = Arc::clone(&named_thread_locals);
        let named_thread_locals_clone_5 = Arc::clone(&named_thread_locals);

        let barrier = Arc::new(Barrier::new(5));
        let barrier_t1 = barrier.clone();
        let barrier_t2 = barrier.clone();
        let barrier_t3 = barrier.clone();
        let barrier_t4 = barrier.clone();

        let sleep_millis: u64 = 40;
        let t_user1_ny_handler = thread::Builder::new()
            .name(thread_name_user1_ny.into())
            .spawn(move || {
                barrier_t1.wait();
                range_t1.with_locked_recursive(user_1_photos_2021_ny_sk.clone(), |_locks| {
                    named_thread_locals_clone_1.start();
                    thread::sleep(Duration::from_millis(sleep_millis / 2));
                    named_thread_locals_clone_1.finish();
                })
            })
            .unwrap();

        let t_user2_videos_2022_handler = thread::Builder::new()
            .name(thread_name_user2_2022.into())
            .spawn(move || {
                barrier_t2.wait();
                range_t2.with_locked(user2_videos_2022_sk.clone(), |_locks| {
                    named_thread_locals_clone_2.start();
                    thread::sleep(Duration::from_millis(sleep_millis + 10));
                    named_thread_locals_clone_2.finish();
                })
            })
            .unwrap();

        let t_etc_handler = thread::Builder::new()
            .name(thread_name_etc.into())
            .spawn(move || {
                barrier_t3.wait();

                thread::sleep(Duration::from_millis(5));
                assert!(!range_t3.locked(etc_sk.clone()));

                range_t3.with_locked(etc_sk.clone(), |_locks| {
                    named_thread_locals_clone_3.start();
                    thread::sleep(Duration::from_millis(sleep_millis));
                    named_thread_locals_clone_3.finish();
                })
            })
            .unwrap();

        let t_pair_handler = thread::Builder::new()
            .name(thread_name_pair_recursive.into())
            .spawn(move || {
                barrier_t4.wait();

                thread::sleep(Duration::from_millis(5));

                range_t4.with_locked_recursive_and_nonrecursive(
                    user2_sk.clone(),
                    user_1_photos_2021_sk,
                    |_locks| {
                        named_thread_locals_clone_4.start();
                        thread::sleep(Duration::from_millis(sleep_millis));
                        named_thread_locals_clone_4.finish();
                    },
                )
            })
            .unwrap();

        let t_final_handler = thread::Builder::new()
            .name(thread_name_final.into())
            .spawn(move || {
                barrier.wait();
                std::thread::sleep(Duration::from_millis(10));
                assert!(range.locked(user_1_photos_2021_tx_sk.clone()));
                thread::sleep(Duration::from_millis(sleep_millis + 20));
                named_thread_locals_clone_5.start();
                assert!(!range.locked(user_1_photos_2021_tx_sk.clone()));
                named_thread_locals_clone_5.finish();
            })
            .unwrap();

        t_final_handler.join().unwrap();
        t_pair_handler.join().unwrap();
        t_etc_handler.join().unwrap();
        t_user2_videos_2022_handler.join().unwrap();
        t_user1_ny_handler.join().unwrap();

        let events = FixedNamedThreadLocals::arc_events(named_thread_locals);

        let user1_ny_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_user1_ny)
            .expect("thread with this name should exist");

        let user2_2022_event =
            CurThreadEvent::find_by_name(events.as_slice(), thread_name_user2_2022)
                .expect("thread with this name should exist");

        let etc_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_etc)
            .expect("thread with this name should exist");

        let pair_recursive_event =
            CurThreadEvent::find_by_name(events.as_slice(), thread_name_pair_recursive)
                .expect("thread with this name should exist");

        let final_event = CurThreadEvent::find_by_name(events.as_slice(), thread_name_final)
            .expect("thread with this name should exist");

        assert_eq!(user1_ny_event.overlaps_with(user2_2022_event), Some(true));
        assert_eq!(
            user1_ny_event.overlaps_with(pair_recursive_event),
            Some(false)
        );
        assert_eq!(etc_event.overlaps_with(pair_recursive_event), Some(true));
        assert_eq!(final_event.overlaps_with(pair_recursive_event), Some(false));
    }

    #[test]
    fn test_delete() {
        let tree: Arc<RangeLock<&'static str>> = Arc::new(RangeLock::new());

        let expire_duration_millis = 50;
        let expire_duration = Duration::from_millis(expire_duration_millis);
        let check_frequency = Duration::from_millis(10);

        RangeLock::start_delete_expired_keys_async(tree.clone(), expire_duration, check_frequency);

        let key_first: SearchKey<_> = Path::new("/home/user1").try_into().unwrap();

        tree.insert_lock(key_first.clone());

        assert!(
            tree.key_exists(&key_first),
            "key_first should exist right after createting"
        );

        std::thread::sleep(Duration::from_millis(expire_duration_millis / 2));

        assert!(
            tree.key_exists(&key_first),
            "key_first should still be present after 1 second"
        );

        let key_last: SearchKey<_> = Path::new("/etc").try_into().unwrap();
        tree.insert_lock(key_last.clone());
        assert!(
            tree.key_exists(&key_last),
            "key_last should be present right after creation"
        );

        std::thread::sleep(Duration::from_millis(expire_duration_millis / 2 + 10));

        assert!(
            !tree.key_exists(&key_first),
            "key_first should be removed after max timeout"
        );

        assert!(
            tree.key_exists(&key_last),
            "key_last should should not expire yet"
        );
        std::thread::sleep(Duration::from_millis(expire_duration_millis / 2));
        assert!(
            !tree.key_exists(&key_last),
            "key_last should be removed after max timeout"
        );
    }
}
