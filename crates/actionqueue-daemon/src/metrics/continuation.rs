//! Read one coherent projection and lifetime telemetry snapshot. Scrapes never observe events.
pub(crate) fn encode(state: &crate::http::RouterStateInner) -> String {
    use std::fmt::Write;
    let Some(a) = &state.control_authority else {
        return String::new();
    };
    let Ok(a) = a.lock() else {
        return String::new();
    };
    let p = a.projection();
    let o = a.telemetry().snapshot();
    let mut out = String::new();
    for (name, value) in [
        ("actionqueue_admission_conflict_total", o.admission_conflicts),
        ("actionqueue_signal_duplicate_total", o.signal_duplicates),
        ("actionqueue_projection_digest_mismatch_total", o.projection_mismatches),
    ] {
        let _ = writeln!(out, "# TYPE {name} counter\n{name} {value}");
    }
    let _=writeln!(out,"# TYPE actionqueue_admission_total counter\nactionqueue_admission_total{{outcome=\"created\"}} {}\nactionqueue_admission_total{{outcome=\"duplicate\"}} {}\nactionqueue_admission_total{{outcome=\"conflict\"}} {}",o.admissions_created,o.admission_duplicates,o.admission_conflicts);
    out.push_str("# TYPE actionqueue_signals_admitted_total counter\n");
    if o.signals.is_empty() {
        out.push_str(
            "actionqueue_signals_admitted_total{namespace=\"overflow\",kind=\"overflow\"} 0\n",
        );
    }
    for ((ns, k), n) in o.signals {
        let _ = writeln!(
            out,
            "actionqueue_signals_admitted_total{{namespace=\"{ns}\",kind=\"{k}\"}} {n}"
        );
    }
    out.push_str("# TYPE actionqueue_waits_satisfied_total counter\n");
    for reason in ["signal", "children", "deadline", "control", "canceled"] {
        let _ = writeln!(
            out,
            "actionqueue_waits_satisfied_total{{reason=\"{reason}\"}} {}",
            o.waits_satisfied.get(reason).unwrap_or(&0)
        );
    }
    for (name, n) in [
        ("actionqueue_waits_active", p.waits().active_count()),
        (
            "actionqueue_runs_awaiting",
            p.run_instances()
                .filter(|r| r.state() == actionqueue_core::run::RunState::Awaiting)
                .count(),
        ),
        (
            "actionqueue_resume_context_pending",
            p.run_instances().filter(|r| p.next_resume_assignment(r.id()).is_some()).count(),
        ),
    ] {
        let _ = writeln!(out, "# TYPE {name} gauge\n{name} {n}");
    }
    let _=writeln!(out,"# TYPE actionqueue_disposition_rejected_total counter\nactionqueue_disposition_rejected_total{{reason=\"rejected\"}} {}\n# TYPE actionqueue_recovery_reconciliations_total counter\nactionqueue_recovery_reconciliations_total{{kind=\"execution\"}} {}",o.disposition_rejected,o.recovery_reconciliations);
    for (name, count, sum) in [
        ("actionqueue_wait_latency_seconds", o.wait_latency_count, o.wait_latency_sum),
        ("actionqueue_compound_record_bytes", o.compound_bytes_count, o.compound_bytes_sum),
    ] {
        let _=writeln!(out,"# TYPE {name} histogram\n{name}_bucket{{le=\"+Inf\"}} {count}\n{name}_count {count}\n{name}_sum {sum}");
    }
    out
}
