//! Independent small-state model: expected states never use the production reducer.
#[path = "harness/engine.rs"]
mod engine;
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
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Command {
    task: u64,
    op: Op,
}
#[derive(Clone)]
struct Model {
    states: Vec<&'static str>,
    signal: bool,
    waits: Vec<bool>,
    resolved: usize,
    failures: Vec<u32>,
    terminal: Vec<Option<&'static str>>,
}
impl Model {
    fn new(n: usize) -> Self {
        Self {
            states: vec!["running"; n],
            signal: false,
            waits: vec![false; n],
            resolved: 0,
            failures: vec![0; n],
            terminal: vec![None; n],
        }
    }
    fn reconcile(&mut self, deadline: bool) {
        for i in 0..self.states.len() {
            if self.states[i] == "awaiting" && (self.signal || deadline) {
                self.states[i] = "ready";
                self.resolved += 1;
            }
        }
    }
    fn apply(&mut self, c: &Command) -> bool {
        let i = c.task as usize - 1;
        match c.op {
            Op::Wait => {
                if self.states[i] != "running" {
                    return false;
                }
                self.states[i] = "awaiting";
                self.waits[i] = true;
            }
            Op::Signal => {
                if !self.signal {
                    self.signal = true;
                    self.reconcile(true);
                }
            }
            Op::Deadline => self.reconcile(true),
            Op::Cancel => {
                if ["canceled", "failed", "completed"].contains(&self.states[i]) {
                    return false;
                }
                if self.states[i] == "awaiting" {
                    self.resolved += 1;
                }
                self.states[i] = "canceled";
                self.terminal[i] = Some("canceled");
            }
            Op::Expire => {
                for j in 0..self.states.len() {
                    if self.states[j] == "running" {
                        self.states[j] = "failed";
                        self.failures[j] = 1;
                        self.terminal[j] = Some("failed");
                    }
                }
            }
            Op::Retry | Op::Conflict | Op::Stale => {}
        }
        true
    }
    fn check(&self, d: &Embedded) {
        let o = d.observe();
        assert_eq!(o["tasks"], self.states.len());
        assert_eq!(o["waits"], self.waits.iter().filter(|b| **b).count());
        assert_eq!(o["resolved_waits"], self.resolved);
        assert_eq!(o["active_waits"], self.states.iter().filter(|s| **s == "awaiting").count());
        for (i, state) in self.states.iter().enumerate() {
            let r = &o["runs"][(i + 1).to_string()];
            assert_eq!(r["state"], *state);
            assert_eq!(r["failures"], self.failures[i]);
            assert_eq!(r["attempts"], 1);
            let run = d.runs[&(i as u64 + 1)];
            let terminals: Vec<_> = d
                .a()
                .projection()
                .get_run_history(&run)
                .unwrap()
                .iter()
                .filter(|h| h.to().is_terminal())
                .map(|h| h.to().label())
                .collect();
            assert_eq!(terminals, self.terminal[i].into_iter().collect::<Vec<_>>());
        }
    }
}
fn execute(n: usize, commands: &[Command]) {
    let dir = tempfile::tempdir().unwrap();
    let mut d = Embedded::new(&dir.path().join("store"));
    for task in 1..=n {
        d.execute(&Step::Start { task: task as u64 });
    }
    let mut model = Model::new(n);
    model.check(&d);
    for c in commands {
        if model.apply(c) {
            match c.op {
                Op::Wait => {
                    d.execute(&Step::Wait { task: c.task, deadline: Some(24) });
                }
                Op::Signal => {
                    d.execute(&Step::Signal { signal: 1 });
                }
                Op::Deadline => {
                    d.execute(&Step::Reconcile { at: 30 });
                }
                Op::Cancel => {
                    d.execute(&Step::Cancel { task: c.task });
                }
                Op::Retry | Op::Conflict => {
                    d.execute(&Step::RetryAdmission {
                        task: c.task,
                        conflict: matches!(c.op, Op::Conflict),
                    });
                }
                Op::Expire => d.expire(2000),
                Op::Stale => d.assert_stale_disposition_rejected(c.task),
            }
        }
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
        std::fs::write(&file,serde_json::to_vec_pretty(&serde_json::json!({"seed":seed,"tasks":n,"commands":commands,"minimized":minimized})).unwrap()).unwrap();
        eprintln!("model failure saved to {}", file.display());
        std::panic::resume_unwind(failure);
    }
}
#[test]
fn exhaustively_enumerates_short_wait_signal_deadline_cancel_races() {
    let ops = [Op::Wait, Op::Signal, Op::Deadline, Op::Cancel];
    for n in 1..=3 {
        for code in 0..64 {
            let mut v = code;
            let mut commands = vec![];
            for index in 0..3 {
                commands.push(Command { task: (index % n + 1) as u64, op: ops[v % 4] });
                v /= 4;
            }
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
    ];
    for seed in 1u64..=16 {
        let mut random = seed;
        let n = (seed as usize % 3) + 1;
        let mut commands = vec![];
        for _ in 0..40 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            commands.push(Command {
                task: random % n as u64 + 1,
                op: ops[(random >> 8) as usize % ops.len()],
            });
        }
        checked(seed, n, commands);
    }
}
