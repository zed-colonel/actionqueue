//! Generic append-only ledger backed by WAL events.

use std::collections::HashMap;

use actionqueue_core::ids::{LedgerEntryId, TenantId};
use actionqueue_core::platform::LedgerEntry;
use tracing;

/// Generic append-only ledger for platform events.
///
/// Ledger keys identify logical ledgers (e.g. `"audit"`, `"decision"`,
/// `"relationship"`, `"incident"`, `"reality"`). Entries are opaque byte
/// payloads whose schema is defined by the consumer (Caelum, Digicorp).
///
/// Maintains secondary indexes for O(N_key) and O(N_tenant) queries.
#[derive(Default)]
pub struct AppendLedger {
    entries: Vec<LedgerEntry>,
    /// Secondary index: ledger_key → Vec of indices into `entries`.
    entries_by_key: HashMap<String, Vec<usize>>,
    /// Secondary index: tenant_id → Vec of indices into `entries`.
    entries_by_tenant: HashMap<TenantId, Vec<usize>>,
}

impl AppendLedger {
    /// Creates an empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends an entry to the ledger.
    pub fn append(&mut self, entry: LedgerEntry) {
        let idx = self.entries.len();
        tracing::debug!(
            entry_id = %entry.entry_id(),
            ledger_key = entry.ledger_key(),
            tenant_id = %entry.tenant_id(),
            "ledger entry appended"
        );
        self.entries_by_key.entry(entry.ledger_key().to_string()).or_default().push(idx);
        self.entries_by_tenant.entry(entry.tenant_id()).or_default().push(idx);
        self.entries.push(entry);
    }

    /// Returns an iterator over all entries for the given ledger key.
    pub fn iter_for_key<'a>(&'a self, key: &str) -> impl Iterator<Item = &'a LedgerEntry> {
        let indices = self.entries_by_key.get(key).map(|v| v.as_slice()).unwrap_or(&[]);
        indices.iter().filter_map(|&i| self.entries.get(i))
    }

    /// Returns an iterator over all entries for the given tenant.
    pub fn iter_for_tenant(&self, tenant_id: TenantId) -> impl Iterator<Item = &LedgerEntry> {
        let indices = self.entries_by_tenant.get(&tenant_id).map(|v| v.as_slice()).unwrap_or(&[]);
        indices.iter().filter_map(|&i| self.entries.get(i))
    }

    /// Returns the entry for the given identifier, if any.
    pub fn entry_by_id(&self, entry_id: LedgerEntryId) -> Option<&LedgerEntry> {
        self.entries.iter().find(|e| e.entry_id() == entry_id)
    }

    /// Returns the total number of entries in the ledger.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the ledger contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns an iterator over all entries, in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = &LedgerEntry> {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{LedgerEntryId, TenantId};
    use actionqueue_core::platform::LedgerEntry;

    use super::AppendLedger;

    fn make_entry(tenant: TenantId, key: &str) -> LedgerEntry {
        LedgerEntry::new(LedgerEntryId::new(), tenant, key, b"payload".to_vec(), 1000)
    }

    #[test]
    fn append_and_len() {
        let mut ledger = AppendLedger::new();
        let tenant = TenantId::new();
        ledger.append(make_entry(tenant, "audit"));
        ledger.append(make_entry(tenant, "decision"));
        assert_eq!(ledger.len(), 2);
    }

    #[test]
    fn iter_for_key_returns_matching_entries() {
        let mut ledger = AppendLedger::new();
        let tenant = TenantId::new();
        ledger.append(make_entry(tenant, "audit"));
        ledger.append(make_entry(tenant, "decision"));
        ledger.append(make_entry(tenant, "audit"));

        let audit_entries: Vec<_> = ledger.iter_for_key("audit").collect();
        assert_eq!(audit_entries.len(), 2);

        let decision_entries: Vec<_> = ledger.iter_for_key("decision").collect();
        assert_eq!(decision_entries.len(), 1);
    }

    #[test]
    fn iter_for_tenant_returns_all_tenant_entries() {
        let mut ledger = AppendLedger::new();
        let tenant_a = TenantId::new();
        let tenant_b = TenantId::new();
        ledger.append(make_entry(tenant_a, "audit"));
        ledger.append(make_entry(tenant_b, "audit"));
        ledger.append(make_entry(tenant_a, "decision"));

        let a_entries: Vec<_> = ledger.iter_for_tenant(tenant_a).collect();
        assert_eq!(a_entries.len(), 2);

        let b_entries: Vec<_> = ledger.iter_for_tenant(tenant_b).collect();
        assert_eq!(b_entries.len(), 1);
    }

    #[test]
    fn entry_by_id_returns_correct_entry() {
        let mut ledger = AppendLedger::new();
        let tenant = TenantId::new();
        let entry_id = LedgerEntryId::new();
        let entry = LedgerEntry::new(entry_id, tenant, "audit", b"data".to_vec(), 1000);
        ledger.append(entry);
        assert!(ledger.entry_by_id(entry_id).is_some());
        assert!(ledger.entry_by_id(LedgerEntryId::new()).is_none());
    }

    #[test]
    fn empty_key_returns_empty_iter() {
        let ledger = AppendLedger::new();
        assert_eq!(ledger.iter_for_key("nonexistent").count(), 0);
    }
}
