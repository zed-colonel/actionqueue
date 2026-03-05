# actionqueue-budget

Budget enforcement, suspend/resume, and event subscriptions for the ActionQueue task queue engine.

## Overview

This crate provides in-memory budget tracking and event subscription matching:

- **BudgetTracker** -- Per-task budget state (allocations, consumption, exhaustion)
- **BudgetGate** -- Pre-dispatch eligibility check against budget limits
- **SubscriptionRegistry** -- Active event subscription state management
- **Event matching** -- ActionQueueEvent-to-subscription filter matching

Budget dimensions: Token, CostCents, TimeSecs. Suspended attempts do not count toward max_attempts. Requires the `budget` feature flag at the workspace level.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

MIT
