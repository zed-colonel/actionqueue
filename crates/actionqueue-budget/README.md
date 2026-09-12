# actionqueue-budget

Budget tracking and dispatch eligibility for ActionQueue. `BudgetTracker` mirrors
storage's complete allocation, consumption, and exhaustion records; `BudgetGate`
blocks leasing while any dimension is exhausted. Dimensions are Token, CostCents,
and TimeSecs. Replenishment replaces the limit and resets consumption.

Running handlers can observe cooperative cancellation and return `Suspended`.
Explicit suspension resume is available without the budget feature. Awaiting runs
hold no execution lease and incur no wall-clock consumption. A matched signal or
deadline commits pending resume input even when budget blocks dispatch; replenishing
budget neither resolves a wait nor resumes a suspended run.

Internal structural subscriptions belong to `actionqueue-engine::reactivity`.
Runtime subscription APIs and persisted records retain the `budget` profile.
External continuation uses durable signal admission and waits, independently.

License: Apache-2.0.
