//! One byte encoder for every canonical representation (admission, signal,
//! disposition). Integers are little endian, strings/bytes carry u64 lengths,
//! options carry a 0/1 tag, and every tag is a protocol constant, never a Rust
//! discriminant. Callers own the domain prefix and version.
use crate::data_ref::DataRef;

pub(crate) struct Encoder(Vec<u8>);
impl Encoder {
    pub(crate) fn new(prefix: &[u8]) -> Self {
        Self(prefix.to_vec())
    }
    pub(crate) fn byte(&mut self, v: u8) {
        self.0.push(v);
    }
    pub(crate) fn u32(&mut self, v: u32) {
        self.0.extend(v.to_le_bytes());
    }
    pub(crate) fn u64(&mut self, v: u64) {
        self.0.extend(v.to_le_bytes());
    }
    pub(crate) fn bytes(&mut self, v: &[u8]) {
        self.u64(v.len() as u64);
        self.0.extend(v);
    }
    /// Appends already-canonical bytes without a length prefix.
    pub(crate) fn raw(&mut self, v: &[u8]) {
        self.0.extend(v);
    }
    pub(crate) fn text(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
    pub(crate) fn uuid(&mut self, v: &uuid::Uuid) {
        self.0.extend(v.as_bytes());
    }
    pub(crate) fn option<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, T)) {
        self.byte(u8::from(v.is_some()));
        if let Some(v) = v {
            f(self, v);
        }
    }
    pub(crate) fn hash(&mut self, h: &crate::bounded::ContentHash) {
        self.byte(1);
        self.bytes(h.bytes());
    }
    pub(crate) fn data(&mut self, value: &DataRef) {
        match value {
            DataRef::Inline(d) => {
                self.byte(0);
                self.option(d.content_type(), |e, v| e.text(v.as_str()));
                self.bytes(d.bytes());
                self.hash(d.hash());
            }
            DataRef::External(d) => {
                self.byte(1);
                self.text(d.scheme.as_str());
                self.text(d.locator.expose());
                self.hash(&d.hash);
                self.option(d.size_bytes, Self::u64);
                self.option(d.content_type.as_ref(), |e, v| e.text(v.as_str()));
            }
        }
    }
    pub(crate) fn finish(self) -> Vec<u8> {
        self.0
    }
}
