//! Deterministic one-shot failure injection, enabled only by test-support builds.
#[cfg(feature = "testing")]
thread_local! { static FAIL: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) }; }
#[cfg(feature = "testing")]
pub fn fail_once(point: &str) {
    FAIL.with(|f| *f.borrow_mut() = Some(point.into()));
}
pub(crate) fn checkpoint(point: &str) -> std::io::Result<()> {
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
