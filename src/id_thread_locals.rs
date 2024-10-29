use std::{borrow::BorrowMut, cell::RefCell, collections::HashMap, sync::RwLock, thread::ThreadId};

use crate::thread_events::{CurThreadEvent, ThreadLocals};

#[allow(unused)]
pub struct IdThreadLocals {
    lock: RwLock<HashMap<ThreadId, RefCell<CurThreadEvent>>>,
}

/**
* SAFETY
* internal HashMap is ptotected by RwLock:
* all changes and iterating over items are protected with write lock
* all reads by read lock
*/
unsafe impl Send for IdThreadLocals {}
unsafe impl Sync for IdThreadLocals {}

#[allow(unused)]
impl IdThreadLocals {
    pub fn new() -> Self {
        Self {
            lock: RwLock::new(HashMap::new()),
        }
    }

    pub fn events(&self) -> Vec<CurThreadEvent> {
        let cur_id = std::thread::current().id();
        let mut write_lock = self
            .lock
            .write()
            .expect("should be able to acquire write lock");

        write_lock
            .borrow_mut()
            .values()
            .map(|e_cell| e_cell.take())
            .collect()
    }
}

#[allow(unused)]
impl ThreadLocals for IdThreadLocals {
    fn start(&self) {
        let cur_thread = std::thread::current();
        let cur_id = cur_thread.id();
        let cur_name = cur_thread.name().map(|name| name.to_owned());

        let mut write_lock = self
            .lock
            .write()
            .expect("should be able to acquire write lock");
        let mut event = CurThreadEvent::new(Some(cur_id), cur_name);
        event.start();

        write_lock.borrow_mut().insert(cur_id, RefCell::new(event));
    }

    fn finish(&self) {
        let cur_id = std::thread::current().id();
        let read_lock = self
            .lock
            .read()
            .expect("should be able to acquire read lock");

        let event_cell = read_lock
            .get(&cur_id)
            .expect("cur thread should call start() before finninsh()");
        event_cell.borrow_mut().finish();
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::thread_events;

    use super::IdThreadLocals;

    #[test]
    fn simple_flow() {
        let tl = Arc::new(IdThreadLocals::new());

        let tl_clone_1 = Arc::clone(&tl);

        let t1_handler = thread_events::spawn(tl_clone_1, move || {
            std::thread::sleep(Duration::from_millis(30));
        });

        let tl_clone_2 = Arc::clone(&tl);
        let t2_handler = thread_events::spawn(tl_clone_2, move || {
            std::thread::sleep(Duration::from_millis(30));
        });

        t1_handler.join().unwrap();
        t2_handler.join().unwrap();
        let events = tl.events();
        assert_eq!(events.len(), 2);
        // dbg!(events);
    }
}
