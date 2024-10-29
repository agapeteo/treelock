use std::{cell::RefCell, collections::HashMap, sync::Arc};

use crate::thread_events::{CurThreadEvent, ThreadLocals};

#[derive(Debug)]
pub struct FixedNamedThreadLocals(*const HashMap<String, RefCell<CurThreadEvent>>);

/**
* SAFETY:
* as internal hashmap structure never changss (fixed) it is safe tpo share between threads.
* values in hashmap can be changes only by same single thread (thread local)
* iterating over elements can be save ONLY when single thread reads it.
*/
unsafe impl Send for FixedNamedThreadLocals {}
unsafe impl Sync for FixedNamedThreadLocals {}

/**
 * SAFETY:
 * inner HashMap structure never changes
 * raw pointer is not null (explicit check above)
 */
impl Drop for FixedNamedThreadLocals {
    fn drop(&mut self) {
        if self.0.is_null() {
            return;
        }
        std::mem::drop(unsafe {
            Box::from_raw(self.0 as *mut HashMap<String, RefCell<CurThreadEvent>>)
        })
    }
}

#[allow(unused)]
impl FixedNamedThreadLocals {
    pub fn new(names: Vec<String>) -> Self {
        let map: HashMap<String, RefCell<CurThreadEvent>> = names
            .into_iter()
            .map(|name| {
                (
                    name.clone(),
                    RefCell::new(CurThreadEvent::new(None, Some(name))),
                )
            })
            .collect();

        Self(Box::into_raw(Box::new(map)))
    }

    pub fn new_arced(names: Vec<String>) -> Arc<Self> {
        Arc::new(Self::new(names))
    }

    pub fn push_note(&self, note: String) {
        let cur_thread = std::thread::current();
        let thread_name = cur_thread
            .name()
            .expect("this implementation expects events for only named thread");

        let map_ref = unsafe { &*self.0 };
        let cell_ref = map_ref
            .get(thread_name)
            .expect("cur thread name should be registered");

        cell_ref.borrow_mut().push_extra(note);
    }

    /**
     * unsafe when more than one thread has access to this object
     * using arc_events methos is recommended or wait for join() completed in all other threads
     */
    pub unsafe fn events(&self) -> Vec<CurThreadEvent> {
        let map_ref = unsafe { &*self.0 };
        map_ref.values().map(|e_cell| e_cell.take()).collect()
    }

    /**
     * using Arc to check if only current thread has access to object
     */
    pub fn arc_events(this: Arc<Self>) -> Vec<CurThreadEvent> {
        if Arc::weak_count(&this) > 0 || Arc::strong_count(&this) > 1 {
            panic!("another thread may use this struct");
        }
        unsafe {
            Arc::into_inner(this)
                .expect("only one thread should call this func")
                .events()
        }
    }
}

#[allow(unused)]
impl ThreadLocals for FixedNamedThreadLocals {
    fn start(&self) {
        let cur_thread = std::thread::current();
        let thread_name = cur_thread
            .name()
            .expect("this implementation expects events for only named thread");

        let map_ref = unsafe { &*self.0 };
        let cell_ref = map_ref
            .get(thread_name)
            .expect("cur thread name should be registered");

        cell_ref.borrow_mut().start();
        cell_ref.borrow_mut().set_id(cur_thread.id());
    }

    fn finish(&self) {
        let cur_thread = std::thread::current();
        let thread_name = cur_thread
            .name()
            .expect("this implementation expects events for only named thread");

        let map_ref = unsafe { &*self.0 };
        let cell_ref = map_ref
            .get(thread_name)
            .expect("cur thread name should be registered");
        cell_ref.borrow_mut().finish();
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::Arc, time::Duration};

    use crate::thread_events;

    use super::FixedNamedThreadLocals;

    #[test]
    fn simple_flow() {
        let (thread_name_1, thread_name_2) = ("t1", "t2");
        let tl =
            FixedNamedThreadLocals::new_arced(vec![thread_name_1.into(), thread_name_2.into()]);

        let tl_clone_1 = Arc::clone(&tl);
        let handler_1 =
            thread_events::spawn_named(thread_name_1.into(), tl_clone_1.clone(), move || {
                std::thread::sleep(Duration::from_millis(40));
                tl_clone_1.push_note("note for t1".into());
            })
            .unwrap();

        let tl_clone_2 = Arc::clone(&tl);
        let handler_2 =
            thread_events::spawn_named(thread_name_2.into(), tl_clone_2.clone(), move || {
                std::thread::sleep(Duration::from_millis(40));
                tl_clone_2.push_note("note for t2".into());
            })
            .unwrap();

        handler_1.join().unwrap();
        handler_2.join().unwrap();

        let events = FixedNamedThreadLocals::arc_events(tl);
        assert_eq!(events.len(), 2);
        let mut event_names: Vec<_> = events
            .iter()
            .map(|event| event.thread_name().as_ref().expect("name expected"))
            .collect();
        event_names.sort();
        assert_eq!(event_names, vec![thread_name_1, thread_name_2]);
        // dbg!(events);
    }
}
