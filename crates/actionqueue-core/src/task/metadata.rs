//! Task metadata for organizing and prioritizing tasks.

/// Metadata associated with a task for organizational and scheduling purposes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TaskMetadata {
    /// Optional tags for categorizing tasks.
    tags: Vec<String>,
    /// Optional priority hint. Higher values indicate higher priority.
    priority: i32,
    /// Optional human-readable description.
    description: Option<String>,
}

impl TaskMetadata {
    /// Creates a new `TaskMetadata` with the given tags, priority, and description.
    pub fn new(tags: Vec<String>, priority: i32, description: Option<String>) -> Self {
        Self { tags, priority, description }
    }

    /// Returns the tags associated with this task.
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Returns the priority hint for this task.
    pub fn priority(&self) -> i32 {
        self.priority
    }

    /// Returns the optional human-readable description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}
