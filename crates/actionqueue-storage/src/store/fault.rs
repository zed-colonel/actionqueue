//! Deterministic, thread-local failure injection in test-support builds only.
#[cfg(feature = "testing")]
thread_local! {
    static FAIL: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    static PAUSE: std::cell::RefCell<Option<Selection>> = const { std::cell::RefCell::new(None) };
    static KIND: std::cell::Cell<Option<u16>> = const { std::cell::Cell::new(None) };
}
#[cfg(feature = "testing")]
struct Selection {
    point: String,
    kind: Option<u16>,
    remaining: usize,
}
#[cfg(feature = "testing")]
pub fn fail_once(point: &str) {
    FAIL.with(|f| *f.borrow_mut() = Some(point.into()));
}
/// Stops at a named boundary until the conformance parent kills this process.
#[cfg(feature = "testing")]
pub fn pause_once(point: &str) {
    pause_on(point, None, 1);
}
/// Selects the Nth matching boundary on the calling thread. Arm inside the
/// blocking worker that executes the mutation, not on its async caller.
/// `kind` is the stable WAL wire kind, not a Rust enum ordinal.
#[cfg(feature = "testing")]
pub fn pause_on(point: &str, kind: Option<u16>, occurrence: usize) {
    assert!(occurrence > 0, "occurrences are one-based");
    PAUSE.with(|p| {
        *p.borrow_mut() = Some(Selection { point: point.into(), kind, remaining: occurrence });
    });
}
/// Restores nesting on every return path, including rejected mutations.
pub(crate) struct EventScope {
    #[cfg(feature = "testing")]
    previous: Option<u16>,
}
pub(crate) fn event_scope(kind: u16) -> EventScope {
    let _ = kind;
    EventScope {
        #[cfg(feature = "testing")]
        previous: KIND.with(|k| k.replace(Some(kind))),
    }
}
impl Drop for EventScope {
    fn drop(&mut self) {
        #[cfg(feature = "testing")]
        KIND.with(|k| k.set(self.previous));
    }
}
#[cfg(feature = "testing")]
pub(crate) fn armed(point: &str) -> bool {
    FAIL.with(|p| p.borrow().as_deref() == Some(point))
        || PAUSE.with(|p| {
            p.borrow().as_ref().is_some_and(|s| {
                s.point == point && (s.kind.is_none() || s.kind == KIND.with(|k| k.get()))
            })
        })
}
pub(crate) fn checkpoint(point: &str) -> std::io::Result<()> {
    #[cfg(feature = "testing")]
    if PAUSE.with(|p| {
        let mut p = p.borrow_mut();
        let Some(s) = p.as_mut() else { return false };
        if s.point != point || (s.kind.is_some() && s.kind != KIND.with(|k| k.get())) {
            return false;
        }
        s.remaining -= 1;
        s.remaining == 0
    }) {
        use std::io::Write;
        println!("AQ_CRASH_BOUNDARY {point} kind={:?}", KIND.with(|k| k.get()));
        std::io::stdout().flush()?;
        loop {
            std::thread::park();
        }
    }
    #[cfg(feature = "testing")]
    if FAIL.with(|f| {
        let mut value = f.borrow_mut();
        if value.as_deref() == Some(point) {
            value.take();
            true
        } else {
            false
        }
    }) {
        return Err(std::io::Error::other(format!("injected failure: {point}")));
    }
    let _ = point;
    Ok(())
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    #[test]
    fn scopes_restore_kind_and_wrong_kinds_do_not_consume_occurrences() {
        pause_on("selected", Some(336), 2);
        {
            let _outer = event_scope(256);
            assert!(!armed("selected"));
            checkpoint("selected").unwrap();
            {
                let _inner = event_scope(336);
                assert!(armed("selected"));
                checkpoint("selected").unwrap();
                PAUSE.with(|p| assert_eq!(p.borrow().as_ref().unwrap().remaining, 1));
            }
            assert!(!armed("selected"));
        }
        assert_eq!(KIND.with(|k| k.get()), None);
        PAUSE.with(|p| *p.borrow_mut() = None);
    }
}
