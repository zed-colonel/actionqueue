//! Budget domain types for resource consumption tracking.
//!
//! These types define the vocabulary for budget allocation and consumption
//! that Caelum uses to enforce per-thread and per-Vessel resource caps.
//! ActionQueue enforces the caps; Caelum aggregates and reports consumption.

/// The dimension along which a budget is measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum BudgetDimension {
    /// Token-count budget (e.g. LLM tokens).
    Token,
    /// Cost budget measured in hundredths of a cent (cost-cents).
    CostCents,
    /// Wall-clock time budget in seconds.
    TimeSecs,
}

impl std::fmt::Display for BudgetDimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            BudgetDimension::Token => "token",
            BudgetDimension::CostCents => "cost_cents",
            BudgetDimension::TimeSecs => "time_secs",
        };
        write!(f, "{name}")
    }
}

/// A unit of resource consumption reported by a handler after execution.
///
/// Handlers attach zero or more `BudgetConsumption` records to their output.
/// The dispatch loop durably records these and updates the in-memory
/// `BudgetTracker` after each attempt completes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BudgetConsumption {
    /// The resource dimension being consumed.
    pub dimension: BudgetDimension,
    /// The amount consumed in this attempt.
    pub amount: u64,
}

impl BudgetConsumption {
    /// Creates a new consumption record.
    pub fn new(dimension: BudgetDimension, amount: u64) -> Self {
        Self { dimension, amount }
    }
}

/// A budget allocation for a single (task, dimension) pair.
///
/// Each task may have at most one allocation per `BudgetDimension`.
/// Validated construction ensures the limit is non-zero.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct BudgetAllocation {
    dimension: BudgetDimension,
    limit: u64,
}

/// Error returned when a `BudgetAllocation` is constructed with invalid values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetAllocationError {
    /// Budget limit must be greater than zero.
    ZeroLimit,
}

impl std::fmt::Display for BudgetAllocationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BudgetAllocationError::ZeroLimit => write!(f, "budget limit must be greater than zero"),
        }
    }
}

impl std::error::Error for BudgetAllocationError {}

impl BudgetAllocation {
    /// Creates a new budget allocation with validation.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetAllocationError::ZeroLimit`] if `limit` is 0.
    pub fn new(dimension: BudgetDimension, limit: u64) -> Result<Self, BudgetAllocationError> {
        if limit == 0 {
            return Err(BudgetAllocationError::ZeroLimit);
        }
        Ok(Self { dimension, limit })
    }

    /// Returns the budget dimension.
    pub fn dimension(&self) -> BudgetDimension {
        self.dimension
    }

    /// Returns the budget limit.
    pub fn limit(&self) -> u64 {
        self.limit
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for BudgetAllocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Wire {
            dimension: BudgetDimension,
            limit: u64,
        }

        let wire = Wire::deserialize(deserializer)?;
        BudgetAllocation::new(wire.dimension, wire.limit)
            .map_err(|_| serde::de::Error::custom("budget limit must be greater than zero"))
    }
}
