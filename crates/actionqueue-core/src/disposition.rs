//! Target attempt dispositions; distinct from the legacy mutation outcome until AQ-08.
use crate::admission::{validate_dependencies, AdmissionRejection};
use crate::bounded::{BoundedCode, BoundedError};
use crate::budget::BudgetConsumption;
use crate::causal::CausalOverride;
use crate::continuation::{CheckpointRef, SignalProposal, WaitSpec};
use crate::data_ref::DataRef;
use crate::ids::{AdmissionKey, TaskId};
use crate::task::task_spec::TaskSpec;
/// Target attempt result taxonomy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DispositionOutcome {
    /// Successful completion.
    Complete,
    /// Failed with retry eligibility.
    RetryableFailure {
        /// Bounded error.
        error: BoundedError,
    },
    /// Failed without retry eligibility.
    TerminalFailure {
        /// Bounded error.
        error: BoundedError,
    },
    /// Execution timed out.
    Timeout {
        /// Bounded error.
        error: BoundedError,
    },
    /// Preempted without consuming the failure cap.
    Suspended {
        /// Optional bounded reason.
        reason: Option<BoundedCode>,
    },
    /// Yielded to a durable continuation.
    Awaiting,
}
/// Uncommitted inputs; only a validated AttemptDisposition may cross the commit seam.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DispositionParts {
    /// Output.
    pub output: Option<DataRef>,
    /// Checkpoint.
    pub checkpoint: Option<CheckpointRef>,
    /// Wait.
    pub wait: Option<WaitSpec>,
    /// Child admissions.
    pub child_admissions: Vec<ChildAdmission>,
    /// Emitted signals.
    pub emitted_signals: Vec<SignalProposal>,
    /// Consumption.
    pub consumption: Vec<BudgetConsumption>,
}
/// Structurally validated attempt effects.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "DispositionWire"))]
pub struct AttemptDisposition {
    /// Outcome.
    outcome: DispositionOutcome,
    /// Output.
    output: Option<DataRef>,
    /// Checkpoint.
    checkpoint: Option<CheckpointRef>,
    /// Wait.
    wait: Option<WaitSpec>,
    /// Child admissions.
    child_admissions: Vec<ChildAdmission>,
    /// Emitted signals.
    emitted_signals: Vec<SignalProposal>,
    /// Consumption.
    consumption: Vec<BudgetConsumption>,
}
/// Invalid combination or collection size in a disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispositionError {
    /// Outcome does not permit the supplied effects.
    InvalidCombination,
    /// Hard collection ceiling exceeded.
    TooLarge,
}
impl std::fmt::Display for DispositionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid disposition: {self:?}")
    }
}
impl std::error::Error for DispositionError {}
impl AttemptDisposition {
    /// Checks outcome/effect combinations and hard collection ceilings.
    pub fn new(
        outcome: DispositionOutcome,
        parts: DispositionParts,
    ) -> Result<Self, DispositionError> {
        if parts.child_admissions.len() > crate::limits::MAX_CHILD_ADMISSIONS_PER_DISPOSITION
            || parts.emitted_signals.len() > crate::limits::MAX_SIGNALS_PER_DISPOSITION
            || parts.consumption.len() > crate::limits::MAX_CONSUMPTION_ENTRIES_PER_DISPOSITION
        {
            return Err(DispositionError::TooLarge);
        }
        let valid = match &outcome {
            DispositionOutcome::Complete => parts.wait.is_none() && parts.checkpoint.is_none(),
            DispositionOutcome::Awaiting => parts.wait.is_some(),
            DispositionOutcome::Suspended { .. } => parts.wait.is_none(),
            DispositionOutcome::RetryableFailure { .. }
            | DispositionOutcome::TerminalFailure { .. }
            | DispositionOutcome::Timeout { .. } => {
                parts.wait.is_none() && parts.child_admissions.is_empty()
            }
        };
        if !valid {
            return Err(DispositionError::InvalidCombination);
        }
        Ok(Self {
            outcome,
            output: parts.output,
            checkpoint: parts.checkpoint,
            wait: parts.wait,
            child_admissions: parts.child_admissions,
            emitted_signals: parts.emitted_signals,
            consumption: parts.consumption,
        })
    }
    /// Returns outcome.
    pub fn outcome(&self) -> &DispositionOutcome {
        &self.outcome
    }
    /// Returns output.
    pub fn output(&self) -> Option<&DataRef> {
        self.output.as_ref()
    }
    /// Returns checkpoint.
    pub fn checkpoint(&self) -> Option<&CheckpointRef> {
        self.checkpoint.as_ref()
    }
    /// Returns wait.
    pub fn wait(&self) -> Option<&WaitSpec> {
        self.wait.as_ref()
    }
    /// Returns child admissions.
    pub fn child_admissions(&self) -> &[ChildAdmission] {
        &self.child_admissions
    }
    /// Returns emitted signals.
    pub fn emitted_signals(&self) -> &[SignalProposal] {
        &self.emitted_signals
    }
    /// Returns consumption.
    pub fn consumption(&self) -> &[BudgetConsumption] {
        &self.consumption
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct DispositionWire {
    /// Outcome.
    outcome: DispositionOutcome,
    /// Output.
    output: Option<DataRef>,
    /// Checkpoint.
    checkpoint: Option<CheckpointRef>,
    /// Wait.
    wait: Option<WaitSpec>,
    /// Child admissions.
    child_admissions: Vec<ChildAdmission>,
    /// Emitted signals.
    emitted_signals: Vec<SignalProposal>,
    /// Consumption.
    consumption: Vec<BudgetConsumption>,
}
#[cfg(feature = "serde")]
impl TryFrom<DispositionWire> for AttemptDisposition {
    type Error = DispositionError;
    fn try_from(w: DispositionWire) -> Result<Self, Self::Error> {
        Self::new(
            w.outcome,
            DispositionParts {
                output: w.output,
                checkpoint: w.checkpoint,
                wait: w.wait,
                child_admissions: w.child_admissions,
                emitted_signals: w.emitted_signals,
                consumption: w.consumption,
            },
        )
    }
}
/// Child task intent with bounded attribution overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "ChildWire"))]
pub struct ChildAdmission {
    /// Admission key.
    admission_key: AdmissionKey,
    /// Task spec.
    task_spec: TaskSpec,
    /// Dependencies.
    dependencies: Vec<TaskId>,
    /// Causal override.
    causal_override: CausalOverride,
}
impl ChildAdmission {
    /// Validates the child specification and dependency list.
    pub fn new(
        admission_key: AdmissionKey,
        task_spec: TaskSpec,
        mut dependencies: Vec<TaskId>,
        causal_override: CausalOverride,
    ) -> Result<Self, AdmissionRejection> {
        validate_dependencies(&task_spec, &mut dependencies)?;
        Ok(Self { admission_key, task_spec, dependencies, causal_override })
    }
    /// Returns admission key.
    pub fn admission_key(&self) -> &AdmissionKey {
        &self.admission_key
    }
    /// Returns task spec.
    pub fn task_spec(&self) -> &TaskSpec {
        &self.task_spec
    }
    /// Returns dependencies.
    pub fn dependencies(&self) -> &[TaskId] {
        &self.dependencies
    }
    /// Returns causal override.
    pub fn causal_override(&self) -> &CausalOverride {
        &self.causal_override
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct ChildWire {
    /// Admission key.
    admission_key: AdmissionKey,
    /// Task spec.
    task_spec: TaskSpec,
    /// Dependencies.
    dependencies: Vec<TaskId>,
    /// Causal override.
    causal_override: CausalOverride,
}
#[cfg(feature = "serde")]
impl TryFrom<ChildWire> for ChildAdmission {
    type Error = AdmissionRejection;
    fn try_from(w: ChildWire) -> Result<Self, Self::Error> {
        Self::new(w.admission_key, w.task_spec, w.dependencies, w.causal_override)
    }
}
