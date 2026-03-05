//! Cycle detection: the dependency gate rejects circular declarations.
//!
//! Proves that `DependencyGate::declare` returns `CycleError` when a cycle
//! would be introduced. Direct cycles (A→B, B→A), transitive cycles
//! (A→B→C→A), self-cycles, and diamond-pattern cycles are all rejected.

mod support;

#[cfg(feature = "workflow")]
mod wf {
    use std::str::FromStr;

    use actionqueue_core::ids::TaskId;
    use actionqueue_workflow::dag::DependencyGate;

    fn tid(n: u8) -> TaskId {
        TaskId::from_str(&format!("000000{n:02x}-0001-0001-0001-000000000001"))
            .expect("fixed task id must parse")
    }

    #[test]
    fn direct_cycle_rejected_at_declaration() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("A depends on B must succeed");
        let err = gate
            .declare(tid(1), vec![tid(2)])
            .expect_err("B depends on A would form a cycle and must be rejected");
        assert_eq!(
            err.task_id(),
            tid(1),
            "cycle error must identify the task whose declaration creates the cycle"
        );
        assert_eq!(
            err.cycle_through(),
            tid(2),
            "cycle error must identify the prerequisite that closes the cycle"
        );
    }

    #[test]
    fn transitive_cycle_rejected_at_declaration() {
        let mut gate = DependencyGate::new();
        // A→B→C declared first.
        gate.declare(tid(2), vec![tid(1)]).expect("B depends on A: no cycle");
        gate.declare(tid(3), vec![tid(2)]).expect("C depends on B: no cycle");
        // Adding A→C would close the cycle A→B→C→A.
        let err = gate
            .declare(tid(1), vec![tid(3)])
            .expect_err("A depends on C would form a transitive cycle A→B→C→A");
        assert_eq!(err.task_id(), tid(1), "cycle error must name the declaring task");
    }

    #[test]
    fn self_cycle_rejected_at_declaration() {
        let mut gate = DependencyGate::new();
        let err = gate
            .declare(tid(1), vec![tid(1)])
            .expect_err("task depending on itself must be rejected");
        assert_eq!(err.task_id(), tid(1));
        assert_eq!(err.cycle_through(), tid(1));
    }

    #[test]
    fn non_cycle_declarations_accepted() {
        let mut gate = DependencyGate::new();
        // Diamond: C and D both depend on B; B depends on A.
        gate.declare(tid(2), vec![tid(1)]).expect("B depends on A: ok");
        gate.declare(tid(3), vec![tid(2)]).expect("C depends on B: ok");
        gate.declare(tid(4), vec![tid(2)]).expect("D depends on B: ok");
        // E depends on both C and D (fan-in): still no cycle.
        gate.declare(tid(5), vec![tid(3), tid(4)]).expect("E depends on C and D: ok");
    }

    #[test]
    fn diamond_back_edge_creates_cycle() {
        let mut gate = DependencyGate::new();
        // Valid diamond: A→B, A→C, B→D, C→D.
        gate.declare(tid(2), vec![tid(1)]).expect("B depends on A: ok");
        gate.declare(tid(3), vec![tid(1)]).expect("C depends on A: ok");
        gate.declare(tid(4), vec![tid(2), tid(3)]).expect("D depends on B and C: ok (diamond)");
        // Back-edge D→A would close a cycle through the diamond.
        let err = gate
            .declare(tid(1), vec![tid(4)])
            .expect_err("A depends on D would form a cycle through the diamond");
        assert_eq!(err.task_id(), tid(1));
    }
}
