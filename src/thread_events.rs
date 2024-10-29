use std::{thread::ThreadId, time::Instant};

#[allow(unused)]
#[derive(Default, Debug)]
pub struct CurThreadEvent {
    id: Option<ThreadId>,
    name: Option<String>,
    start: Option<Instant>,
    finish: Option<Instant>,
    extra: Vec<(String, Instant)>,
}

impl CurThreadEvent {
    pub fn new(id: Option<ThreadId>, name: Option<String>) -> Self {
        Self {
            id,
            name,
            ..Default::default()
        }
    }

    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    pub fn set_id(&mut self, id: ThreadId) {
        self.id = Some(id);
    }

    pub fn finish(&mut self) {
        self.finish = Some(Instant::now());
    }

    pub fn push_extra(&mut self, note: String) {
        self.extra.push((note, Instant::now()));
    }

    pub fn thread_id(&self) -> Option<ThreadId> {
        self.id
    }

    pub fn thread_name(&self) -> &Option<String> {
        &self.name
    }

    pub fn overlaps_with(&self, other: &CurThreadEvent) -> Option<bool> {
        let (first, second) = if self.start? <= other.start? {
            (self, other)
        } else {
            (other, self)
        };

        let first_finish = first.finish?;

        Some(first_finish >= second.start?)
    }

    pub fn contains_any_overlap(slice: &mut [Self]) -> bool {
        slice.sort_by(|a, b| {
            a.start
                .expect("event should be started")
                .cmp(&b.start.expect("event should be started"))
        });
        for i in 0..slice.len() - 1 {
            let left = &slice[i];
            let right = &slice[i + 1];
            if left.overlaps_with(right) == Some(true) {
                return true;
            }
        }
        false
    }

    pub fn find_by_id(slice: &[Self], thread_id: ThreadId) -> Option<&Self> {
        slice.iter().find(|e| {
            if let Some(id) = e.id {
                return id == thread_id;
            }
            false
        })
    }

    pub fn find_by_name<'a>(slice: &'a [Self], thread_name: &str) -> Option<&'a Self> {
        slice.iter().find(|e| {
            if let Some(name) = e.name.as_ref() {
                return name.as_str() == thread_name;
            }
            false
        })
    }
}

pub trait ThreadLocals {
    fn start(&self);
    fn finish(&self);
}

#[allow(unused)]
pub fn spawn<F, T, E>(events_tracker: std::sync::Arc<E>, f: F) -> std::thread::JoinHandle<T>
where
    E: ThreadLocals + Send + Sync + 'static,
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        events_tracker.start();
        let res = f();
        events_tracker.finish();
        res
    })
}

#[allow(unused)]
pub fn spawn_named<F, T, E>(
    name: String,
    events_tracker: std::sync::Arc<E>,
    f: F,
) -> Result<std::thread::JoinHandle<T>, std::io::Error>
where
    E: ThreadLocals + Send + Sync + 'static,
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new().name(name).spawn(move || {
        events_tracker.start();
        let res = f();
        events_tracker.finish();
        res
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::id_thread_locals::IdThreadLocals;

    use super::CurThreadEvent;

    fn new_thread_event() -> CurThreadEvent {
        let cur_thread = std::thread::current();
        let mut event = CurThreadEvent::new(Some(cur_thread.id()), None);
        event.start();
        event
    }

    #[test]
    fn oeverlaps_with() {
        let mut first = new_thread_event();
        first.finish();

        let mut second = new_thread_event();
        second.finish();

        assert_eq!(first.overlaps_with(&second), Some(false));

        let mut first = new_thread_event();
        let mut second = new_thread_event();
        first.finish();
        second.finish();

        assert_eq!(first.overlaps_with(&second), Some(true));
    }

    #[test]
    fn test_contains_overlaps() {
        let mut first = new_thread_event();
        first.finish();

        let mut second = new_thread_event();
        let mut third = new_thread_event();
        third.finish();
        second.finish();

        assert!(second.overlaps_with(&third) == Some(true));
        assert!(CurThreadEvent::contains_any_overlap(&mut [
            first, second, third
        ]));
    }

    #[test]
    fn test_thread_overlap() {
        let events = std::sync::Arc::new(IdThreadLocals::new());
        let events_t1 = events.clone();
        let events_t2 = events.clone();

        let t1 = super::spawn(events_t1, move || {
            std::thread::sleep(std::time::Duration::from_millis(40));
        });

        let t2 = super::spawn(events_t2, move || {
            std::thread::sleep(std::time::Duration::from_millis(40));
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let mut thread_events = events.events();
        assert!(CurThreadEvent::contains_any_overlap(
            thread_events.as_mut_slice()
        ));
    }

    #[test]
    fn test_thread_no_overlap() {
        let events = std::sync::Arc::new(IdThreadLocals::new());
        let events_t1 = events.clone();
        let events_t2 = events.clone();

        let t1 = super::spawn(events_t1, move || {
            std::thread::sleep(std::time::Duration::from_millis(40));
        });
        t1.join().unwrap();

        let t2 = super::spawn(events_t2, move || {
            std::thread::sleep(std::time::Duration::from_millis(40));
        });

        t2.join().unwrap();

        let mut thread_events = events.events();
        assert!(!CurThreadEvent::contains_any_overlap(
            thread_events.as_mut_slice()
        ));
    }

    #[test]
    fn event_by_id() {
        let events = std::sync::Arc::new(IdThreadLocals::new());
        let events_t1 = events.clone();

        let id = std::sync::Mutex::new(None);
        let id_arc = Arc::new(id);
        let t_arc = Arc::clone(&id_arc);

        super::spawn(events_t1, move || {
            let cur_id = std::thread::current().id();
            let mut gaurd_id = t_arc.lock().unwrap();
            *gaurd_id = Some(cur_id);
        })
        .join()
        .unwrap();

        let id = id_arc.lock().unwrap().unwrap();
        let thread_events = events.events();
        let event = CurThreadEvent::find_by_id(&thread_events, id);
        assert!(event.is_some());
    }

    #[test]
    fn event_by_name() {
        let events = std::sync::Arc::new(IdThreadLocals::new());
        let events_t1 = events.clone();
        let thread_name = "test_thread_name";

        super::spawn_named(thread_name.to_owned(), events_t1, move || {})
            .unwrap()
            .join()
            .unwrap();

        let thread_events = events.events();
        let event = CurThreadEvent::find_by_name(&thread_events, thread_name);
        assert!(event.is_some());
    }
}
