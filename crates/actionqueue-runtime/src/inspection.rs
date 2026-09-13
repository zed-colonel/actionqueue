//! Authorized, bounded structural inspection against a single immutable projection revision.
use actionqueue_core::{continuation::*, control::*, ids::*};
use actionqueue_storage::{
    mutation::control::authorize_projection,
    recovery::{inspection::ReferenceField, reducer::ReplayReducer},
};

use crate::views::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectionError {
    Unauthorized,
    NotFound,
    InvalidQuery,
    StaleCursor,
    TooLarge,
}
impl InspectionError {
    pub fn code(self) -> &'static str {
        match self {
            Self::Unauthorized => "forbidden",
            Self::NotFound => "not_found",
            Self::InvalidQuery => "invalid_query",
            Self::StaleCursor => "stale_cursor",
            Self::TooLarge => "response_too_large",
        }
    }
}
impl std::fmt::Display for InspectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for InspectionError {}
/// Set only by trusted host code. Query parameters cannot grant disclosure.
#[derive(Debug, Clone, Copy, Default)]
pub struct DisclosurePolicy {
    pub allow_references: bool,
}
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub origin_ref: Option<String>,
    pub edge_cursor: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    #[serde(default)]
    pub display_references: bool,
}
/// A host must supply the real store profile; this is never request input.
pub struct Inspector<'a> {
    p: &'a ReplayReducer,
    host: &'a HostControlContext,
    platform: bool,
    disclose: bool,
    now: u64,
}
impl<'a> Inspector<'a> {
    // Preserve the explicit dependencies of this existing boundary API.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        p: &'a ReplayReducer,
        host: &'a HostControlContext,
        platform: bool,
        policy: DisclosurePolicy,
        request_references: bool,
        now: u64,
    ) -> Result<Self, InspectionError> {
        if request_references && !policy.allow_references {
            return Err(InspectionError::Unauthorized);
        }
        Ok(Self { p, host, platform, disclose: request_references && policy.allow_references, now })
    }
    fn scope(&self, action: QueueAction) -> Result<Option<TenantId>, InspectionError> {
        authorize_projection(self.p, self.platform, self.host, action)
            .map_err(|_| InspectionError::Unauthorized)
    }
    fn task_scope(&self, id: TaskId, action: QueueAction) -> Result<(), InspectionError> {
        let tenant = self.scope(action)?;
        self.p
            .get_task(&id)
            .filter(|t| t.tenant_id() == tenant)
            .ok_or(InspectionError::NotFound)
            .map(|_| ())
    }
    fn run_scope(&self, id: RunId, action: QueueAction) -> Result<(), InspectionError> {
        self.scope(action)?;
        let r = self.p.get_run_instance(&id).ok_or(InspectionError::NotFound)?;
        self.task_scope(r.task_id(), action)
    }
    fn reference(&self, value: Option<&str>) -> ReferenceView {
        ReferenceView::new(value, self.disclose)
    }
    fn causation(&self, c: &actionqueue_core::causal::CausationLink) -> CausationView {
        let visible = self.scope(QueueAction::InspectTask).is_ok()
            && c.parent_task_id()
                .is_none_or(|id| self.task_scope(id, QueueAction::InspectTask).is_ok())
            && c.parent_run_id()
                .is_none_or(|id| self.run_scope(id, QueueAction::InspectTask).is_ok());
        CausationView {
            task_links_visible: visible,
            task_id: c.parent_task_id().filter(|_| visible),
            run_id: c.parent_run_id().filter(|_| visible),
            attempt_id: c.parent_attempt_id().filter(|_| visible),
            external_ref: self.reference(c.external_ref().map(|r| r.expose())),
        }
    }
    fn control(&self, sequence: u64) -> Option<ControlView> {
        self.p.control_history().get(&sequence).map(|c| ControlView {
            sequence,
            actor_id: c.actor_id,
            scope: c.scope,
            caller_ref: self.reference(Some(c.context.caller_ref().expose())),
            host_session_ref: self.reference(c.context.host_session_ref().map(|r| r.expose())),
            request_id: self.reference(c.context.request_id().map(|r| r.expose())),
        })
    }
    fn admission_view(
        &self,
        r: &actionqueue_storage::mutation::admission::AdmissionRecord,
    ) -> AdmissionView {
        let c = r.request().causal_context();
        AdmissionView {
            control: self.control(r.sequence()),
            key: r.key().clone(),
            task_id: r.task_id(),
            digest: r.digest().clone(),
            sequence: r.sequence(),
            timestamp: r.timestamp(),
            dependencies: r.request().dependencies().to_vec(),
            causal: CausalView {
                causation: c.causation().map(|c| self.causation(c)),
                trace_id: self.reference(Some(c.trace_id().as_str())),
                correlation_id: self.reference(Some(c.correlation_id().as_str())),
                origin_ref: self.reference(c.origin_ref().map(|v| v.expose())),
                submitting_principal_ref: self
                    .reference(c.submitting_principal_ref().map(|v| v.expose())),
                requesting_actor_ref: self.reference(c.requesting_actor_ref().map(|v| v.expose())),
                purpose_ref: self.reference(c.purpose_ref().map(|v| v.expose())),
                authorization_ref: self.reference(c.authorization_ref().map(|v| v.expose())),
                identity_context_ref: self.reference(c.identity_context_ref().map(|v| v.expose())),
                signed_statement_ref: self.reference(c.signed_statement_ref().map(|v| v.expose())),
                proof_context_ref: self.reference(c.proof_context_ref().map(|v| v.expose())),
            },
        }
    }
    #[cfg(feature = "serde")]
    pub fn task_controls(
        &self,
        id: TaskId,
        q: &Query,
    ) -> Result<ControlHistoryView, InspectionError> {
        self.task_scope(id, QueueAction::InspectTask)?;
        self.control_page(
            actionqueue_storage::recovery::inspection::ControlTarget::Task(id),
            q,
            &format!("task-controls/{id}"),
        )
    }
    #[cfg(feature = "serde")]
    pub fn run_controls(
        &self,
        id: RunId,
        q: &Query,
    ) -> Result<ControlHistoryView, InspectionError> {
        self.run_scope(id, QueueAction::InspectTask)?;
        self.control_page(
            actionqueue_storage::recovery::inspection::ControlTarget::Run(id),
            q,
            &format!("run-controls/{id}"),
        )
    }
    #[cfg(feature = "serde")]
    fn control_page(
        &self,
        target: actionqueue_storage::recovery::inspection::ControlTarget,
        q: &Query,
        lane: &str,
    ) -> Result<ControlHistoryView, InspectionError> {
        Ok(ControlHistoryView {
            available: self.p.control_targets_available(),
            entries: self.page(
                q,
                lane,
                self.p.control_sequences(target).filter_map(|s| self.control(s)).map(Ok),
            )?,
        })
    }
    pub fn get_admission(&self, key: &AdmissionKey) -> Result<AdmissionView, InspectionError> {
        let tenant = self.scope(QueueAction::InspectTask)?;
        self.p
            .admission(tenant, key)
            .map(|r| self.admission_view(r))
            .ok_or(InspectionError::NotFound)
    }
    pub fn get_task(&self, id: TaskId) -> Result<TaskView, InspectionError> {
        self.task_scope(id, QueueAction::InspectTask)?;
        let r = self.p.get_task_record(&id).ok_or(InspectionError::NotFound)?;
        let t = r.task_spec();
        let mut budgets: Vec<_> = self
            .p
            .budgets()
            .filter(|((task, _), _)| *task == id)
            .map(|(_, b)| BudgetView {
                dimension: b.dimension,
                limit: b.limit,
                consumed: b.consumed,
                exhausted: b.exhausted,
            })
            .collect();
        budgets.sort_by_key(|b| b.dimension.to_string());
        Ok(TaskView {
            last_control: self.p.task_control_sequence(id).and_then(|s| self.control(s)),
            id,
            payload: DataSummary::bytes(t.payload(), t.content_type()),
            priority: t.metadata().priority(),
            constraints: t.constraints().clone(),
            run_policy: t.run_policy().clone(),
            parent_task_id: t.parent_task_id(),
            created_at: r.created_at(),
            canceled_at: r.canceled_at(),
            admission: self.p.task_admission(id).map(|a| self.admission_view(a)),
            budgets,
        })
    }
    fn resume_view(&self, c: ResumeContext) -> ResumeView {
        let wake = match &c.wake {
            WakeReason::Signal { envelope, .. } => {
                WakeView::Signal { data: envelope.payload.as_ref().map(DataSummary::from) }
            }
            WakeReason::Children { outcomes, .. } => WakeView::Children {
                outcomes: outcomes
                    .iter()
                    .filter(|o| self.task_scope(o.task_id, QueueAction::InspectTask).is_ok())
                    .cloned()
                    .collect(),
            },
            WakeReason::Deadline { deadline_at, .. } => {
                WakeView::Deadline { deadline_at: *deadline_at }
            }
            WakeReason::ControlResolution { .. } => WakeView::ControlResolution,
            WakeReason::AdministrativeResume { .. } => WakeView::AdministrativeResume,
        };
        ResumeView {
            context_id: c.context_id,
            wait_id: c.wait_id(),
            checkpoint_id: c.checkpoint.as_ref().map(|v| v.checkpoint_id),
            checkpoint_data: c.checkpoint.as_ref().map(|v| DataSummary::from(&v.data)),
            signal_sequence: match c.wake {
                WakeReason::Signal { signal_sequence, .. } => Some(signal_sequence),
                _ => None,
            },
            resumed_at: c.resumed_at,
            control: self.control(c.context_id.0),
            wake,
        }
    }
    pub fn get_run(&self, id: RunId) -> Result<RunView, InspectionError> {
        self.run_scope(id, QueueAction::InspectTask)?;
        let can_wait = self.scope(QueueAction::InspectWait).is_ok();
        let r = self.p.get_run_instance(&id).ok_or(InspectionError::NotFound)?;
        let mut gates = crate::claim::eligibility(self.p, id, None, self.now);
        gates.retain(|r| *r != crate::claim::EligibilityReason::Executor);
        let can_resume = can_wait && self.scope(QueueAction::InspectSignal).is_ok();
        Ok(RunView {
            resume_context_visible: can_resume,
            pending_resume_context: self
                .p
                .pending_resume(id)
                .filter(|_| can_resume)
                .map(|c| self.resume_view(c)),
            continuation_visible: can_wait,
            concurrency_key: self
                .p
                .get_task(&r.task_id())
                .and_then(|t| t.constraints().concurrency_key())
                .map(str::to_owned),
            block_reason: if r.state().is_terminal() {
                Some("terminal")
            } else if r.state() != actionqueue_core::run::RunState::Ready {
                Some(r.state().label())
            } else if gates.contains(&crate::claim::EligibilityReason::Budget) {
                Some("budget")
            } else {
                None
            },
            #[cfg(feature = "serde")]
            state_history: self.run_history(id, &Query::default())?,
            #[cfg(feature = "serde")]
            attempts: self.list_attempts(id, &Query::default())?,
            run_id: id,
            task_id: r.task_id(),
            state: r.state(),
            scheduled_at: r.scheduled_at(),
            created_at: r.created_at(),
            attempt_count: r.attempt_count(),
            failure_attempt_count: r.failure_attempt_count(),
            current_attempt_id: r.current_attempt_id(),
            lease: self.p.get_lease_metadata(&id).map(|l| LeaseView {
                updated_at: l.updated_at(),
                owner: self.reference(Some(l.owner())),
                expiry: l.expiry(),
                acquired_at: l.acquired_at(),
                granted_at_sequence: l.granted_at_sequence(),
            }),
            gates,
            executor_evaluated: false,
            active_wait_id: if can_wait {
                self.p.waits().active(id).map(|w| w.spec.wait_id())
            } else {
                None
            },
            last_wait_id: if can_wait {
                self.p
                    .waits()
                    .records()
                    .filter(|w| w.run_id == id)
                    .max_by_key(|w| w.sequence)
                    .map(|w| w.spec.wait_id())
            } else {
                None
            },
            pending_resume: if can_wait { self.p.next_resume_assignment(id) } else { None },
        })
    }
    #[cfg(feature = "serde")]
    pub fn run_history(
        &self,
        run: RunId,
        q: &Query,
    ) -> Result<Page<StateTransitionView>, InspectionError> {
        self.run_scope(run, QueueAction::InspectTask)?;
        self.page(
            q,
            &format!("history/{run}"),
            self.p.get_run_history(&run).unwrap_or_default().iter().map(|h| {
                Ok(StateTransitionView { from: h.from(), to: h.to(), timestamp: h.timestamp() })
            }),
        )
    }
    #[cfg(feature = "serde")]
    pub fn list_attempts(
        &self,
        run: RunId,
        q: &Query,
    ) -> Result<Page<AttemptView>, InspectionError> {
        self.run_scope(run, QueueAction::InspectTask)?;
        self.page(
            q,
            &format!("attempts/{run}"),
            self.p
                .get_attempt_history(&run)
                .unwrap_or_default()
                .iter()
                .map(|a| self.get_attempt(run, a.attempt_id())),
        )
    }
    pub fn get_attempt(
        &self,
        run: RunId,
        attempt: AttemptId,
    ) -> Result<AttemptView, InspectionError> {
        self.run_scope(run, QueueAction::InspectTask)?;
        if self.p.attempt_owner(attempt) != Some(run) {
            return Err(InspectionError::NotFound);
        }
        let can_wait = self.scope(QueueAction::InspectWait).is_ok();
        let can_signal = self.scope(QueueAction::InspectSignal).is_ok();
        let a = self
            .p
            .get_attempt_history(&run)
            .and_then(|items| items.iter().find(|a| a.attempt_id() == attempt))
            .ok_or(InspectionError::NotFound)?;
        let resume = self
            .p
            .attempt_resume(run, attempt)
            .filter(|_| can_wait && can_signal)
            .map(|c| self.resume_view(c));
        let mut admitted_children: Vec<_> = self
            .p
            .admissions()
            .filter(|a| {
                a.request().causal_context().causation().is_some_and(|c| {
                    c.parent_run_id() == Some(run) && c.parent_attempt_id() == Some(attempt)
                })
            })
            .map(|a| a.task_id())
            .filter(|id| self.task_scope(*id, QueueAction::InspectTask).is_ok())
            .collect();
        admitted_children.sort();
        if admitted_children.len() > 1000 {
            return Err(InspectionError::TooLarge);
        }
        let history = self.p.get_attempt_history(&run).unwrap_or_default();
        let previous_attempt_id = history
            .iter()
            .position(|a| a.attempt_id() == attempt)
            .and_then(|pos| pos.checked_sub(1))
            .map(|pos| history[pos].attempt_id());
        Ok(AttemptView {
            previous_attempt_id,
            continuation_visible: can_wait && can_signal,
            signal_links_visible: can_signal,
            admitted_children,
            emitted_signals: if can_signal {
                self.p
                    .signals()
                    .records()
                    .filter(|s| {
                        s.envelope().causation.as_ref().is_some_and(|c| {
                            c.parent_run_id() == Some(run) && c.parent_attempt_id() == Some(attempt)
                        })
                    })
                    .map(|s| s.sequence())
                    .collect()
            } else {
                Vec::new()
            },
            run_id: run,
            attempt_id: attempt,
            started_at: a.started_at(),
            finished_at: a.finished_at(),
            result: a.result(),
            finish_origin: a.finish_origin(),
            accepted_start_sequence: a.accepted_start().map(|s| s.sequence),
            assignment: if can_wait { a.accepted_start().and_then(|s| s.assignment) } else { None },
            error: ReferenceView::new(a.error(), false),
            output: a.output_ref().map(DataSummary::from),
            resume,
            checkpoints: self
                .p
                .checkpoints_by_producer(run, attempt)
                .map(|c| c.checkpoint.checkpoint_id)
                .collect(),
        })
    }
    pub fn get_wait(&self, id: WaitId) -> Result<WaitView, InspectionError> {
        self.scope(QueueAction::InspectWait)?;
        let w = self.p.waits().get(id).ok_or(InspectionError::NotFound)?;
        self.run_scope(w.run_id, QueueAction::InspectWait)?;
        let target = match w.spec.target() {
            WaitTarget::Signal { filter, match_policy, eligible_from } => WaitTargetView::Signal {
                namespace: filter.namespace.clone(),
                kind: filter.kind.clone(),
                correlation_id: self.reference(filter.correlation_id.as_ref().map(|v| v.as_str())),
                source_ref: self.reference(filter.source_ref.as_ref().map(|v| v.expose())),
                eligible_from: eligible_from.clone(),
                match_policy: match_policy.clone(),
            },
            WaitTarget::Children { task_ids, policy } => {
                for id in task_ids {
                    self.task_scope(*id, QueueAction::InspectTask)?;
                }
                WaitTargetView::Children { task_ids: task_ids.clone(), policy: *policy }
            }
        };
        let resolution = w.resolution.as_ref().map(|r| {
            use actionqueue_storage::mutation::wait::WaitResolutionKind as K;
            let (reason, signal_sequence) = match r.kind {
                K::Signal(s) => (ResolutionReason::Signal, Some(s)),
                K::Children(_) => (ResolutionReason::Children, None),
                K::Deadline => (ResolutionReason::Deadline, None),
                K::Control(_) => (ResolutionReason::Control, None),
                K::Canceled(_) => (ResolutionReason::Canceled, None),
            };
            ResolutionView {
                signal_visible: self.scope(QueueAction::InspectSignal).is_ok(),
                control: self.control(r.sequence),
                sequence: r.sequence,
                timestamp: r.timestamp,
                reason,
                signal_sequence: if self.scope(QueueAction::InspectSignal).is_ok() {
                    signal_sequence
                } else {
                    None
                },
            }
        });
        Ok(WaitView {
            wait_id: id,
            run_id: w.run_id,
            attempt_id: w.attempt_id,
            sequence: w.sequence,
            timestamp: w.timestamp,
            target,
            deadline: w.spec.deadline().cloned(),
            checkpoint_id: w.checkpoint.as_ref().map(|c| c.checkpoint_id),
            resolution,
        })
    }
    pub fn get_signal(&self, id: &SignalId) -> Result<SignalView, InspectionError> {
        let tenant = self.scope(QueueAction::InspectSignal)?;
        let r = self.p.signals().get_signal(tenant, id).ok_or(InspectionError::NotFound)?;
        let e = r.envelope();
        Ok(SignalView {
            control: self.control(r.wal_sequence()),
            causation: e.causation.as_ref().map(|c| self.causation(c)),
            signal_id: e.signal_id.clone(),
            sequence: r.sequence(),
            wal_sequence: r.wal_sequence(),
            namespace: e.namespace.clone(),
            kind: e.kind.clone(),
            correlation_id: self.reference(e.correlation_id.as_ref().map(|v| v.as_str())),
            source_ref: self.reference(e.source_ref.as_ref().map(|v| v.expose())),
            data: e.payload.as_ref().map(DataSummary::from),
            payload_hash: e.payload_hash.clone(),
            received_at: e.received_at,
            occurred_at: e.occurred_at,
            retained: r.is_retained(),
            pin_count: self.scope(QueueAction::InspectWait).ok().map(|_| r.pins().len()),
        })
    }
    pub fn get_checkpoint(&self, id: CheckpointId) -> Result<CheckpointView, InspectionError> {
        self.scope(QueueAction::InspectTask)?;
        let c = self.p.checkpoint(id).ok_or(InspectionError::NotFound)?;
        self.run_scope(c.run_id, QueueAction::InspectTask)?;
        Ok(CheckpointView {
            checkpoint_id: id,
            run_id: c.run_id,
            attempt_id: c.attempt_id,
            sequence: c.sequence,
            data: DataSummary::from(&c.checkpoint.data),
        })
    }
    #[cfg(feature = "serde")]
    pub fn linked_waits(
        &self,
        id: &SignalId,
        q: &Query,
    ) -> Result<Page<WaitView>, InspectionError> {
        let tenant = self.scope(QueueAction::InspectSignal)?;
        self.scope(QueueAction::InspectWait)?;
        let signal = self.p.signals().get_signal(tenant, id).ok_or(InspectionError::NotFound)?;
        let mut waits: Vec<_> = self.p.waits().records().filter(|w| {
            w.resolution.as_ref().is_some_and(|r| {
                matches!(r.kind, actionqueue_storage::mutation::wait::WaitResolutionKind::Signal(seq) if seq == signal.sequence())
            })
        }).collect();
        waits.sort_by_key(|w| w.sequence);
        self.page(
            q,
            &format!("signal-waits/{}", signal.sequence().get()),
            waits.into_iter().map(|w| self.get_wait(w.spec.wait_id())),
        )
    }
    #[cfg(feature = "serde")]
    pub fn checkpoint_consumers(
        &self,
        id: CheckpointId,
        q: &Query,
    ) -> Result<Page<AttemptView>, InspectionError> {
        let c = self.get_checkpoint(id)?;
        self.page(
            q,
            &format!("checkpoint-consumers/{id}"),
            self.p
                .get_attempt_history(&c.run_id)
                .unwrap_or_default()
                .iter()
                .filter(|a| {
                    self.p
                        .attempt_resume(c.run_id, a.attempt_id())
                        .is_some_and(|r| r.checkpoint.is_some_and(|c| c.checkpoint_id == id))
                })
                .map(|a| self.get_attempt(c.run_id, a.attempt_id())),
        )
    }
    fn selected_tasks(
        &self,
        q: &Query,
    ) -> Result<std::collections::BTreeSet<TaskId>, InspectionError> {
        let tenant = self.scope(QueueAction::InspectTask)?;
        let fields = [
            (ReferenceField::Trace, q.trace_id.as_deref()),
            (ReferenceField::Correlation, q.correlation_id.as_deref()),
            (ReferenceField::Origin, q.origin_ref.as_deref()),
        ];
        if fields.iter().filter(|(_, v)| v.is_some()).count() > 1
            || fields.iter().any(|(_, v)| v.is_some_and(|v| v.is_empty() || v.len() > 4096))
        {
            return Err(InspectionError::InvalidQuery);
        }
        Ok(if let Some((field, Some(value))) = fields.iter().find(|(_, v)| v.is_some()) {
            self.p.tasks_by_reference(tenant, *field, value).collect()
        } else {
            self.p.tasks().filter(|t| t.tenant_id() == tenant).map(|t| t.id()).collect()
        })
    }
    #[cfg(feature = "serde")]
    fn page<T: serde::Serialize>(
        &self,
        q: &Query,
        lane: &str,
        values: impl IntoIterator<Item = Result<T, InspectionError>>,
    ) -> Result<Page<T>, InspectionError> {
        use sha2::Digest;
        let limit = q.limit.unwrap_or(100);
        if limit == 0 || limit > 1000 {
            return Err(InspectionError::InvalidQuery);
        }
        let mut filter = q.clone();
        filter.cursor = None;
        filter.edge_cursor = None;
        let fingerprint = format!(
            "{:x}",
            sha2::Sha256::digest(
                serde_json::to_vec(&(lane, self.host.scope, &filter, self.disclose))
                    .map_err(|_| InspectionError::InvalidQuery)?
            )
        );
        let revision = self.p.latest_sequence();
        let offset = if let Some(cursor) = &q.cursor {
            let parts: Vec<_> = cursor.split(':').collect();
            if parts.len() != 3 || parts[1] != fingerprint {
                return Err(InspectionError::InvalidQuery);
            }
            if parts[0].parse::<u64>().map_err(|_| InspectionError::InvalidQuery)? != revision {
                return Err(InspectionError::StaleCursor);
            }
            parts[2].parse::<usize>().map_err(|_| InspectionError::InvalidQuery)?
        } else {
            0
        };
        let mut values = values.into_iter().skip(offset).peekable();
        let mut items = Vec::new();
        let mut bytes = 0;
        while items.len() < limit {
            let Some(value) = values.next() else {
                break;
            };
            let value = value?;
            bytes += serde_json::to_vec(&value).map_err(|_| InspectionError::TooLarge)?.len();
            if bytes > 1024 * 1024 {
                return Err(InspectionError::TooLarge);
            }
            items.push(value);
        }
        let next_cursor =
            values.peek().map(|_| format!("{revision}:{fingerprint}:{}", offset + items.len()));
        Ok(Page { items, next_cursor, revision })
    }
    #[cfg(feature = "serde")]
    pub fn list_tasks(&self, q: &Query) -> Result<Page<TaskView>, InspectionError> {
        self.page(q, "tasks", self.selected_tasks(q)?.into_iter().map(|id| self.get_task(id)))
    }
    #[cfg(feature = "serde")]
    pub fn list_runs(&self, q: &Query) -> Result<Page<RunView>, InspectionError> {
        let tasks = self.selected_tasks(q)?;
        let mut ids: Vec<_> = self
            .p
            .run_instances()
            .filter(|r| tasks.contains(&r.task_id()))
            .map(|r| r.id())
            .collect();
        ids.sort();
        self.page(q, "runs", ids.into_iter().map(|id| self.get_run(id)))
    }
    #[cfg(feature = "serde")]
    pub fn list_waits(&self, q: &Query) -> Result<Page<WaitView>, InspectionError> {
        let tenant = self.scope(QueueAction::InspectWait)?;
        // Reference filters inspect task admission content and need its permission.
        // An unfiltered wait listing only needs the wait's tenant scope.
        let tasks = if q.trace_id.is_some() || q.correlation_id.is_some() || q.origin_ref.is_some()
        {
            self.selected_tasks(q)?
        } else {
            self.p.tasks().filter(|t| t.tenant_id() == tenant).map(|t| t.id()).collect()
        };
        let mut waits: Vec<_> = self
            .p
            .waits()
            .records()
            .filter(|w| {
                self.p.get_run_instance(&w.run_id).is_some_and(|r| tasks.contains(&r.task_id()))
            })
            .collect();
        waits.sort_by_key(|w| (w.sequence, w.spec.wait_id()));
        self.page(q, "waits", waits.into_iter().map(|w| self.get_wait(w.spec.wait_id())))
    }
    #[cfg(feature = "serde")]
    pub fn list_signals(&self, q: &Query) -> Result<Page<SignalView>, InspectionError> {
        let tenant = self.scope(QueueAction::InspectSignal)?;
        if q.trace_id.is_some() || q.origin_ref.is_some() {
            return Err(InspectionError::InvalidQuery);
        }
        let correlation = q
            .correlation_id
            .as_ref()
            .map(CorrelationId::new)
            .transpose()
            .map_err(|_| InspectionError::InvalidQuery)?;
        let records: Box<
            dyn Iterator<Item = &actionqueue_storage::mutation::signal::SignalRecord> + '_,
        > = if let Some(id) = &correlation {
            Box::new(self.p.signals().correlation_records(tenant, id))
        } else {
            Box::new(self.p.signals().records().filter(move |r| r.envelope().tenant_id == tenant))
        };
        self.page(q, "signals", records.map(|r| self.get_signal(&r.envelope().signal_id)))
    }
    #[cfg(feature = "serde")]
    pub fn trace(&self, q: &Query) -> Result<TraceView, InspectionError> {
        let tenant = self.scope(QueueAction::InspectTask)?;
        self.scope(QueueAction::InspectWait)?;
        self.scope(QueueAction::InspectSignal)?;
        let mut tasks = self.selected_tasks(q)?;
        // Bound closure work as well as output. No partially authorized identifiers escape.
        loop {
            if tasks.len() > 10000 {
                return Err(InspectionError::TooLarge);
            }
            let before = tasks.len();
            for t in self.p.tasks().filter(|t| t.tenant_id() == tenant) {
                if t.parent_task_id().is_some_and(|id| tasks.contains(&id)) {
                    tasks.insert(t.id());
                }
            }
            if before == tasks.len() {
                break;
            }
        }
        let mut nodes = Vec::new();
        let mut signals = std::collections::BTreeSet::new();
        for id in &tasks {
            nodes.push(TraceNode::Task(self.get_task(*id)?));
            let mut runs = self.p.run_ids_for_task(*id);
            runs.sort();
            for run in runs {
                nodes.push(TraceNode::Run(self.get_run(run)?));
                for a in self.p.get_attempt_history(&run).unwrap_or_default() {
                    let view = self.get_attempt(run, a.attempt_id())?;
                    signals.extend(view.emitted_signals.iter().copied());
                    nodes.push(TraceNode::Attempt(view));
                    for c in self.p.checkpoints_by_producer(run, a.attempt_id()) {
                        nodes.push(TraceNode::Checkpoint(
                            self.get_checkpoint(c.checkpoint.checkpoint_id)?,
                        ));
                    }
                }
                let mut waits: Vec<_> =
                    self.p.waits().records().filter(|w| w.run_id == run).collect();
                waits.sort_by_key(|w| w.sequence);
                for w in waits {
                    let view = self.get_wait(w.spec.wait_id())?;
                    if let Some(s) = view.resolution.as_ref().and_then(|r| r.signal_sequence) {
                        signals.insert(s);
                    }
                    nodes.push(TraceNode::Wait(view));
                }
                if nodes.len() > 10000 {
                    return Err(InspectionError::TooLarge);
                }
            }
        }
        if let Some(id) = &q.correlation_id {
            let id = CorrelationId::new(id).map_err(|_| InspectionError::InvalidQuery)?;
            signals.extend(
                self.p.signals().correlation_records(tenant, &id).take(10001).map(|r| r.sequence()),
            );
        }
        if nodes.len() + signals.len() > 10000 {
            return Err(InspectionError::TooLarge);
        }
        for seq in signals {
            let s = self.p.signals().by_sequence(seq).ok_or(InspectionError::NotFound)?;
            nodes.push(TraceNode::Signal(self.get_signal(&s.envelope().signal_id)?));
        }
        let mut edges = Vec::new();
        for node in &nodes {
            use NodeIdentity as N;
            let mut edge = |from, to, kind| edges.push(TraceEdge { from, to, kind });
            match node {
                TraceNode::Task(t) => {
                    if let Some(parent) = t.parent_task_id {
                        edge(N::Task(parent), N::Task(t.id), EdgeKind::Parent);
                    }
                    if let Some(a) = &t.admission {
                        for dep in &a.dependencies {
                            edge(N::Task(*dep), N::Task(t.id), EdgeKind::Dependency);
                        }
                    }
                }
                TraceNode::Run(r) => {
                    edge(N::Task(r.task_id), N::Run(r.run_id), EdgeKind::ScheduledRun)
                }
                TraceNode::Attempt(a) => {
                    let attempt = N::Attempt { run_id: a.run_id, attempt_id: a.attempt_id };
                    edge(N::Run(a.run_id), attempt.clone(), EdgeKind::PhysicalAttempt);
                    if let Some(previous) = a.previous_attempt_id {
                        edge(
                            N::Attempt { run_id: a.run_id, attempt_id: previous },
                            attempt.clone(),
                            EdgeKind::PreviousAttempt,
                        );
                    }
                    if let Some(checkpoint) = a.resume.as_ref().and_then(|r| r.checkpoint_id) {
                        edge(
                            N::Checkpoint(checkpoint),
                            attempt.clone(),
                            EdgeKind::ConsumedCheckpoint,
                        );
                    }
                    for child in &a.admitted_children {
                        edge(attempt.clone(), N::Task(*child), EdgeKind::AdmittedChild);
                    }
                    for signal in &a.emitted_signals {
                        edge(attempt.clone(), N::Signal(*signal), EdgeKind::EmittedSignal);
                    }
                }
                TraceNode::Wait(w) => {
                    edge(
                        N::Attempt { run_id: w.run_id, attempt_id: w.attempt_id },
                        N::Wait(w.wait_id),
                        EdgeKind::EstablishedWait,
                    );
                    if let Some(signal) = w.resolution.as_ref().and_then(|r| r.signal_sequence) {
                        edge(N::Signal(signal), N::Wait(w.wait_id), EdgeKind::ObservedSignal);
                    }
                }
                TraceNode::Checkpoint(c) => edge(
                    N::Attempt { run_id: c.run_id, attempt_id: c.attempt_id },
                    N::Checkpoint(c.checkpoint_id),
                    EdgeKind::ProducedCheckpoint,
                ),
                TraceNode::Signal(_) => {}
            }
        }
        let mut edge_query = q.clone();
        edge_query.cursor = q.edge_cursor.clone();
        let edges = self.page(&edge_query, "trace_edges", edges.into_iter().map(Ok))?;
        let nodes = self.page(q, "trace", nodes.into_iter().map(Ok))?;
        let tasks: Vec<_> = nodes
            .items
            .iter()
            .filter_map(|n| if let TraceNode::Task(t) = n { Some(t) } else { None })
            .collect();
        let mut different_fields = Vec::new();
        if let Some(first) = tasks.first() {
            if tasks.iter().any(|t| t.priority != first.priority) {
                different_fields.push(SchedulingField::Priority);
            }
            if tasks.iter().any(|t| t.constraints != first.constraints) {
                different_fields.push(SchedulingField::Constraints);
            }
            if tasks.iter().any(|t| t.run_policy != first.run_policy) {
                different_fields.push(SchedulingField::RunPolicy);
            }
            if tasks.iter().any(|t| t.budgets != first.budgets) {
                different_fields.push(SchedulingField::Budget);
            }
        }
        if let Some(first) = tasks.first() {
            let schedule = |task: TaskId| {
                let mut times: Vec<_> = self
                    .p
                    .run_instances()
                    .filter(|r| r.task_id() == task)
                    .map(|r| r.scheduled_at())
                    .collect();
                times.sort();
                times
            };
            let deadlines = |task: TaskId| {
                let mut values: Vec<_> = self
                    .p
                    .waits()
                    .records()
                    .filter(|w| {
                        self.p.get_run_instance(&w.run_id).is_some_and(|r| r.task_id() == task)
                    })
                    .map(|w| serde_json::to_string(&w.spec.deadline()).expect("typed deadline"))
                    .collect();
                values.sort();
                values
            };
            if tasks.iter().any(|t| schedule(t.id) != schedule(first.id)) {
                different_fields.push(SchedulingField::RunSchedule);
            }
            if tasks.iter().any(|t| deadlines(t.id) != deadlines(first.id)) {
                different_fields.push(SchedulingField::WaitDeadline);
            }
        }
        Ok(TraceView { notice: TRACE_NOTICE, nodes, edges, different_fields })
    }
}
