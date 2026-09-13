// Operations deliberately separate mutation plumbing from the pure expected model.
impl Embedded {
    pub fn model_start(&mut self, task: u64) {
        let a = self.authority.as_mut().unwrap();
        let q = admission_support::request(task);
        let mut t = q.task_spec().clone();
        t.set_run_policy(RunPolicy::Once).unwrap();
        t.set_constraints(TaskConstraints::new(3, None, None).unwrap()).unwrap();
        admission_support::ensure(a, admission_support::with_spec(&q, t), 10).unwrap();
        let run = a.projection().runs_for_task(admission_support::id(task)).next().unwrap().id();
        transition(a, run, RunState::Ready, 11);
        lease(a, run, 12);
        start(a, run, 13);
        self.runs.insert(task, run);
    }
    pub fn model_wait(&mut self, task: u64, now: u64, children: bool) {
        let a = self.authority.as_mut().unwrap();
        let run = self.runs[&task];
        if children {
            let p = parent(a, run);
            let cs = vec![
                child(2, vec![], ChildLifecyclePolicy::Required, p),
                child(3, vec![], ChildLifecyclePolicy::Required, p),
            ];
            let ids = cs.iter().map(|c| c.task_spec().id()).collect();
            let d = child_disposition(a, run, cs, ids, ChildWaitPolicy::AllTerminal);
            put(a, run, d, now);
            for n in [2, 3] {
                self.runs.insert(
                    n,
                    a.projection().runs_for_task(admission_support::id(n)).next().unwrap().id(),
                );
            }
        } else {
            let mut c = command(
                a,
                run,
                spec(
                    WaitId::new(),
                    Some(WaitDeadline {
                        at: now + 5,
                        policy: WaitTimeoutPolicy::ResumeWithTimeout,
                    }),
                ),
            );
            c.timestamp = now;
            c.checkpoint = Some(checkpoint(a, run, b"model checkpoint"));
            establish(a, c).unwrap();
        }
    }
    pub fn model_signal(&mut self, now: u64) {
        s::admit(self.authority.as_mut().unwrap(), 1, now).unwrap();
    }
    pub fn model_cancel(&mut self, task: u64, now: u64) {
        let a = self.authority.as_mut().unwrap();
        let _ = apply(
            a,
            MutationCommand::Cancel(CancelCommand {
                expected_sequence: seq(a),
                target: CancelTarget::Run(self.runs[&task]),
                tenant_id: None,
                control_context: None,
                timestamp: now,
            }),
        );
    }
    pub fn model_dispatch(&mut self, task: u64, now: u64) {
        let a = self.authority.as_mut().unwrap();
        let run = self.runs[&task];
        if a.projection().get_run_state(&run) != Some(&RunState::Ready) {
            transition(a, run, RunState::Ready, now);
        }
        lease(a, run, now);
        start(a, run, now);
    }
    pub fn model_complete(&mut self, task: u64, now: u64) {
        let a = self.authority.as_mut().unwrap();
        put(a, self.runs[&task], AttemptDisposition::complete(None), now);
    }
}
