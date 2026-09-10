//! Deterministic one-shot failure injection, enabled only by test-support builds.
#[cfg(feature = "testing")]
thread_local! { static FAIL: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) }; }
#[cfg(feature = "testing")]
pub fn fail_once(point: &str) {
    FAIL.with(|f| *f.borrow_mut() = Some(point.into()));
}
#[cfg(feature = "testing")]
thread_local! { static PAUSE: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) }; }
/// Stops at a named boundary until the conformance parent kills this process.
#[cfg(feature = "testing")]
pub fn pause_once(point: &str) {
    PAUSE.with(|p| *p.borrow_mut() = Some(point.into()));
}
#[cfg(feature = "testing")]
pub(crate) fn armed(point: &str) -> bool {
    FAIL.with(|p| p.borrow().as_deref() == Some(point))
        || PAUSE.with(|p| p.borrow().as_deref() == Some(point))
}
pub(crate) fn checkpoint(point: &str) -> std::io::Result<()> {
    #[cfg(feature = "testing")]
    if PAUSE.with(|p| p.borrow().as_deref() == Some(point)) {
        use std::io::Write;
        println!("AQ_CRASH_BOUNDARY {point}");
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
