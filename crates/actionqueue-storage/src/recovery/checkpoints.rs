//! Immutable checkpoint registry derived atomically from wait establishment.
use actionqueue_core::{
    continuation::CheckpointRef,
    ids::{AttemptId, CheckpointId, RunId},
};

use super::reducer::{ReplayReducer, ReplayReducerError};

/// Producing operation and immutable checkpoint data, retained with history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointRecord {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub sequence: u64,
    pub checkpoint: CheckpointRef,
}
impl ReplayReducer {
    pub fn checkpoint(&self, id: CheckpointId) -> Option<&CheckpointRecord> {
        self.checkpoints.get(&id)
    }
    pub fn checkpoints_by_producer(
        &self,
        run: RunId,
        attempt: AttemptId,
    ) -> impl Iterator<Item = &CheckpointRecord> {
        self.checkpoints.values().filter(move |c| c.run_id == run && c.attempt_id == attempt)
    }
    pub(crate) fn index_checkpoint(
        &mut self,
        w: &crate::mutation::wait::WaitRecord,
    ) -> Result<(), ReplayReducerError> {
        if let Some(c) = &w.checkpoint {
            self.index_produced_checkpoint(w.run_id, w.attempt_id, w.sequence, c)?;
        }
        Ok(())
    }
    pub(crate) fn index_produced_checkpoint(
        &mut self,
        run_id: RunId,
        attempt_id: AttemptId,
        sequence: u64,
        c: &CheckpointRef,
    ) -> Result<(), ReplayReducerError> {
        {
            if c.checkpoint_id.is_nil()
                || c.created_by_attempt != attempt_id
                || c.data.validate().is_err()
                || self.checkpoints.contains_key(&c.checkpoint_id)
            {
                return Err(ReplayReducerError::CorruptedData);
            }
            self.checkpoints.insert(
                c.checkpoint_id,
                CheckpointRecord {
                    run_id: run_id,
                    attempt_id: attempt_id,
                    sequence: sequence,
                    checkpoint: c.clone(),
                },
            );
        }
        Ok(())
    }
}
