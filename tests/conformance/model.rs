//! Pure expected state machine; production reducers are used only for actual observations.
#[path = "harness/engine.rs"]
mod engine;
use actionqueue_core::{continuation::*, run::RunState};
use actionqueue_storage::mutation::wait::{WaitRecord, WaitResolutionKind};
use engine::{Embedded, Step};
use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Op {
    Wait,
    Signal,
    Deadline,
    Cancel,
    Retry,
    Conflict,
    Expire,
    Stale,
    Dispatch,
    Complete,
    Fanout,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Command {
    task: u64,
    op: Op,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Winner {
    Signal,
    Deadline,
    Children,
    Canceled,
}
#[derive(Clone)]
struct Task {
    state: RunState,
    attempts: usize,
    max_attempts: u32,
    failures: u32,
    lease: bool,
    waited: bool,
    child_wait: bool,
    deadline: u64,
    winner: Option<Winner>,
    pending: bool,
    // Each physical attempt's independently predicted delivery and predecessor.
    deliveries: Vec<Option<ResumeDelivery>>,
    recovered: bool,
    terminal: Vec<RunState>,
}
impl Task {
    fn new(running: bool) -> Self {
        Self {
            state: if running { RunState::Running } else { RunState::Scheduled },
            attempts: usize::from(running),
            max_attempts: if running { 3 } else { 1 },
            failures: 0,
            lease: running,
            waited: false,
            child_wait: false,
            deadline: 0,
            winner: None,
            pending: false,
            deliveries: if running { vec![None] } else { vec![] },
            recovered: false,
            terminal: vec![],
        }
    }
}
struct Model {
    tasks: Vec<Task>,
    signal: bool,
    now: u64,
}
impl Model {
    fn reconcile(&mut self) {
        let children_done =
            self.tasks.len() == 3 && self.tasks[1..].iter().all(|t| t.state.is_terminal());
        for t in &mut self.tasks {
            if t.state != RunState::Awaiting {
                continue;
            }
            let winner = if t.child_wait {
                children_done.then_some(Winner::Children)
            } else if self.signal {
                Some(Winner::Signal)
            } else {
                (self.now >= t.deadline).then_some(Winner::Deadline)
            };
            if let Some(w) = winner {
                t.winner = Some(w);
                t.state = RunState::Ready;
                t.pending = true;
            }
        }
    }
    fn apply(&mut self, c: &Command, d: &mut Embedded) {
        if c.task as usize > self.tasks.len() {
            return;
        }
        self.now += if matches!(c.op, Op::Expire) { 2000 } else { 1 };
        let now = self.now;
        let i = c.task as usize - 1;
        if i >= self.tasks.len() {
            return;
        }
        let task_count = self.tasks.len();
        let t = &mut self.tasks[i];
        match c.op {
            Op::Wait | Op::Fanout
                if t.state == RunState::Running
                    && !t.waited
                    && (i != 0 || !matches!(c.op, Op::Fanout) || task_count == 1) =>
            {
                let child = matches!(c.op, Op::Fanout);
                if child && (i != 0 || task_count != 1) {
                    return;
                }
                let t = &mut self.tasks[i];
                t.waited = true;
                t.child_wait = child;
                t.deadline = now + 5;
                t.state = RunState::Awaiting;
                t.lease = false;
                d.model_wait(c.task, now, child);
                if child {
                    self.tasks.extend([Task::new(false), Task::new(false)]);
                }
            }
            Op::Signal => {
                d.model_signal(now);
                if !self.signal {
                    self.signal = true;
                    self.reconcile();
                }
            }
            Op::Deadline => {
                self.now += 6;
                self.reconcile();
                d.execute(&Step::Reconcile { at: self.now });
            }
            Op::Cancel if !t.state.is_terminal() => {
                if t.state == RunState::Awaiting {
                    t.winner = Some(Winner::Canceled);
                }
                t.state = RunState::Canceled;
                t.pending = false;
                t.lease = false;
                t.terminal.push(RunState::Canceled);
                d.model_cancel(c.task, now);
            }
            Op::Expire => {
                for t in &mut self.tasks {
                    if t.state == RunState::Running {
                        t.failures += 1;
                        t.lease = false;
                        t.recovered = true;
                        t.state = if t.failures < t.max_attempts {
                            RunState::RetryWait
                        } else {
                            RunState::Failed
                        };
                        if t.state.is_terminal() {
                            t.terminal.push(t.state);
                        }
                        t.pending = !t.state.is_terminal()
                            && t.deliveries.last().is_some_and(Option::is_some);

                        // Children use the default single-failure cap.
                    }
                }
                d.expire(now);
            }
            Op::Dispatch
                if matches!(
                    t.state,
                    RunState::Ready | RunState::RetryWait | RunState::Scheduled
                ) =>
            {
                let delivery = if t.pending && t.recovered {
                    Some(ResumeDelivery::Recovery)
                } else if t.pending {
                    Some(ResumeDelivery::Initial)
                } else {
                    None
                };
                t.pending = false;
                t.state = RunState::Running;
                t.lease = true;
                t.attempts += 1;
                t.deliveries.push(delivery);
                t.recovered = false;
                d.model_dispatch(c.task, now);
            }
            Op::Complete
                if t.state == RunState::Running
                    && (!t.child_wait || t.winner == Some(Winner::Children)) =>
            {
                t.state = RunState::Completed;
                t.lease = false;
                t.terminal.push(RunState::Completed);
                d.model_complete(c.task, now);
            }
            Op::Retry | Op::Conflict
                if !t.child_wait
                    && (i == 0
                        || d.a()
                            .projection()
                            .get_task(&engine::reference_request(c.task).task_spec().id())
                            .unwrap()
                            .parent_task_id()
                            .is_none()) =>
            {
                d.execute(&Step::RetryAdmission {
                    task: c.task,
                    conflict: matches!(c.op, Op::Conflict),
                });
            }
            Op::Stale if t.attempts > 0 => d.assert_stale_disposition_rejected(c.task),
            _ => {}
        }
    }
    fn check(&self, d: &Embedded) {
        let p = d.a().projection();
        assert_eq!(p.task_count(), self.tasks.len());
        for (i, t) in self.tasks.iter().enumerate() {
            let run = d.runs[&(i as u64 + 1)];
            let r = p.get_run_instance(&run).unwrap();
            assert_eq!(r.state(), t.state, "task {}", i + 1);
            assert_eq!(r.attempt_count() as usize, t.attempts);
            assert_eq!(r.failure_attempt_count(), t.failures);
            assert_eq!(p.get_lease_metadata(&run).is_some(), t.lease);
            assert_eq!(p.pending_resume(run).is_some(), t.pending);
            let waits: Vec<_> = p.waits().records().filter(|w| w.run_id == run).collect();
            assert_eq!(waits.len(), usize::from(t.waited));
            if let Some(w) = waits.first() {
                let actual = w.resolution.as_ref().map(|r| match r.kind {
                    WaitResolutionKind::Signal(_) => Winner::Signal,
                    WaitResolutionKind::Deadline => Winner::Deadline,
                    WaitResolutionKind::Children(_) => Winner::Children,
                    WaitResolutionKind::Canceled(_) => Winner::Canceled,
                    _ => panic!("unexpected control winner"),
                });
                assert_eq!(actual, t.winner);
                let cp = w.checkpoint.as_ref().unwrap();
                assert_eq!(cp.created_by_attempt, w.attempt_id);
                assert!(cp
                    .data
                    .verify_bytes(if t.child_wait {
                        b"retained child IDs and next batch"
                    } else {
                        b"model checkpoint"
                    })
                    .is_ok());
                if t.child_wait {
                    assert_eq!(
                        p.get_task(&engine::reference_request(2).task_spec().id())
                            .unwrap()
                            .parent_task_id(),
                        Some(r.task_id())
                    );
                }
                if let Some(context) = p.pending_resume(run) {
                    assert_eq!(context.checkpoint, w.checkpoint);
                    check_wake(&context, t.winner.unwrap(), w);
                }
            }
            let h = p.get_attempt_history(&run).unwrap_or(&[]);
            for (j, a) in h.iter().enumerate() {
                let assignment = a.accepted_start().and_then(|a| a.assignment);
                assert_eq!(assignment.map(|a| a.delivery), t.deliveries[j]);
                if let Some(assignment) = assignment {
                    let w = waits[0];
                    assert_eq!(assignment.context_id.0, w.resolution.as_ref().unwrap().sequence);
                    assert_eq!(
                        assignment.previous_attempt_id,
                        if t.deliveries[j] == Some(ResumeDelivery::Recovery) {
                            Some(h[j - 1].attempt_id())
                        } else {
                            None
                        }
                    );
                    let context = p.attempt_resume(run, a.attempt_id()).unwrap();
                    assert_eq!(context.checkpoint, w.checkpoint);
                    check_wake(&context, t.winner.unwrap(), w);
                }
            }
            let terminals: Vec<_> = p
                .get_run_history(&run)
                .unwrap()
                .iter()
                .filter(|h| h.to().is_terminal())
                .map(|h| h.to())
                .collect();
            assert_eq!(terminals, t.terminal);
        }
    }
}
fn check_wake(c: &ResumeContext, winner: Winner, w: &WaitRecord) {
    match (&c.wake, winner) {
        (WakeReason::Signal { wait_id, signal_sequence, envelope }, Winner::Signal) => {
            assert_eq!(*wait_id, w.spec.wait_id());
            assert_eq!(signal_sequence.get(), 1);
            assert_eq!(envelope.signal_id.as_str(), "signal/1");
        }
        (WakeReason::Deadline { wait_id, deadline_at }, Winner::Deadline) => {
            assert_eq!(*wait_id, w.spec.wait_id());
            assert_eq!(*deadline_at, w.spec.deadline().unwrap().at);
        }
        (WakeReason::Children { wait_id, outcomes }, Winner::Children) => {
            assert_eq!(*wait_id, w.spec.wait_id());
            assert_eq!(outcomes.len(), 2);
        }
        _ => panic!("wrong resume winner"),
    }
}
fn execute(n: usize, commands: &[Command]) {
    let dir = tempfile::tempdir().unwrap();
    let mut d = Embedded::new(&dir.path().join("store"));
    for task in 1..=n {
        d.model_start(task as u64);
    }
    let mut model = Model { tasks: vec![Task::new(true); n], signal: false, now: 20 };
    model.check(&d);
    for c in commands {
        model.apply(c, &mut d);
        model.check(&d);
        d.verify();
    }
}
fn checked(seed: u64, n: usize, commands: Vec<Command>) {
    if let Err(failure) = std::panic::catch_unwind(|| execute(n, &commands)) {
        let mut minimized = commands.clone();
        let mut i = 0;
        while i < minimized.len() {
            let mut candidate = minimized.clone();
            candidate.remove(i);
            if std::panic::catch_unwind(|| execute(n, &candidate)).is_err() {
                minimized = candidate;
            } else {
                i += 1;
            }
        }
        let file = std::env::temp_dir().join(format!("aq-model-failure-{seed}.json"));
        std::fs::write(file,serde_json::to_vec_pretty(&serde_json::json!({"seed":seed,"tasks":n,"commands":commands,"minimized":minimized})).unwrap()).unwrap();
        std::panic::resume_unwind(failure);
    }
}
#[test]
fn exhaustively_enumerates_short_wait_signal_deadline_cancel_races() {
    let ops = [Op::Wait, Op::Signal, Op::Deadline, Op::Cancel];
    for n in 1..=3 {
        for code in 0..64 {
            let mut v = code;
            let commands = (0..3)
                .map(|i| {
                    let c = Command { task: (i % n + 1) as u64, op: ops[v % 4] };
                    v /= 4;
                    c
                })
                .collect();
            checked(code as u64, n, commands);
        }
    }
}
#[test]
fn seeded_long_sequences_include_conflicts_leases_and_stale_results() {
    let ops = [
        Op::Wait,
        Op::Signal,
        Op::Deadline,
        Op::Cancel,
        Op::Retry,
        Op::Conflict,
        Op::Expire,
        Op::Stale,
        Op::Dispatch,
        Op::Complete,
        Op::Fanout,
    ];
    for seed in 1u64..=32 {
        let mut r = seed;
        let commands = (0..50)
            .map(|_| {
                r ^= r << 13;
                r ^= r >> 7;
                r ^= r << 17;
                Command { task: r % 3 + 1, op: ops[(r >> 8) as usize % ops.len()] }
            })
            .collect();
        checked(seed, (seed as usize % 3) + 1, commands);
    }
}
#[test]
fn physical_recovery_redelivers_original_wake_and_children_release_parent() {
    for winner in [Op::Signal, Op::Deadline] {
        checked(
            100,
            1,
            vec![Op::Wait, winner, Op::Dispatch, Op::Expire, Op::Stale, Op::Dispatch, Op::Complete]
                .into_iter()
                .map(|op| Command { task: 1, op })
                .collect(),
        );
    }
    checked(
        101,
        1,
        vec![
            (1, Op::Fanout),
            (2, Op::Dispatch),
            (2, Op::Complete),
            (1, Op::Deadline),
            (3, Op::Dispatch),
            (3, Op::Complete),
            (1, Op::Deadline),
            (1, Op::Dispatch),
            (1, Op::Complete),
        ]
        .into_iter()
        .map(|(task, op)| Command { task, op })
        .collect(),
    );
}
