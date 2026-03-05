//! Property-based tests for WAL codec roundtrip and CRC integrity.

use actionqueue_core::ids::RunId;
use actionqueue_core::run::state::RunState;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::wal::codec::{decode, encode, HEADER_LEN};
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use proptest::prelude::*;

/// Generate a valid (previous_state, new_state) pair for RunStateChanged events.
fn arb_state_pair() -> impl Strategy<Value = (RunState, RunState)> {
    prop_oneof![
        Just((RunState::Scheduled, RunState::Ready)),
        Just((RunState::Scheduled, RunState::Canceled)),
        Just((RunState::Ready, RunState::Leased)),
        Just((RunState::Ready, RunState::Canceled)),
        Just((RunState::Leased, RunState::Running)),
        Just((RunState::Leased, RunState::Ready)),
        Just((RunState::Leased, RunState::Canceled)),
        Just((RunState::Running, RunState::RetryWait)),
        Just((RunState::Running, RunState::Completed)),
        Just((RunState::Running, RunState::Failed)),
        Just((RunState::Running, RunState::Canceled)),
        Just((RunState::RetryWait, RunState::Ready)),
        Just((RunState::RetryWait, RunState::Failed)),
        Just((RunState::RetryWait, RunState::Canceled)),
    ]
}

fn arb_wal_event() -> impl Strategy<Value = WalEvent> {
    (1u64..10_000, arb_state_pair()).prop_map(|(seq, (prev, new))| {
        WalEvent::new(
            seq,
            WalEventType::RunStateChanged {
                run_id: RunId::new(),
                previous_state: prev,
                new_state: new,
                timestamp: 1_000_000,
            },
        )
    })
}

fn config() -> ProptestConfig {
    ProptestConfig::with_cases(256)
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn encode_decode_roundtrip(event in arb_wal_event()) {
        let encoded = encode(&event).expect("encode must succeed");
        let decoded = decode(&encoded).expect("decode must succeed");
        prop_assert_eq!(event, decoded);
    }

    #[test]
    fn crc_detects_single_bit_flip(event in arb_wal_event()) {
        let encoded = encode(&event).expect("encode must succeed");

        // Flip one bit in a payload byte (past the header)
        if encoded.len() > HEADER_LEN {
            let bit_index = HEADER_LEN; // flip first payload byte
            let mut corrupted = encoded.clone();
            corrupted[bit_index] ^= 0x01;
            prop_assert!(
                decode(&corrupted).is_err(),
                "single-bit flip in payload must cause decode failure"
            );
        }
    }

    #[test]
    fn same_events_produce_same_reducer_state(
        seq1 in 1u64..5000,
        seq2 in 5001u64..10000,
        pair1 in arb_state_pair(),
        pair2 in arb_state_pair(),
    ) {
        let run_id = RunId::new();

        let events = vec![
            WalEvent::new(
                seq1,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: pair1.0,
                    new_state: pair1.1,
                    timestamp: 1_000,
                },
            ),
            WalEvent::new(
                seq2,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: pair2.0,
                    new_state: pair2.1,
                    timestamp: 2_000,
                },
            ),
        ];

        let mut reducer1 = ReplayReducer::new();
        let mut reducer2 = ReplayReducer::new();

        // Apply events to both reducers (ignoring errors from invalid transitions
        // since we're testing determinism, not validity).
        for event in &events {
            let _ = reducer1.apply(event);
            let _ = reducer2.apply(event);
        }

        prop_assert_eq!(
            reducer1.latest_sequence(),
            reducer2.latest_sequence(),
            "two reducers replaying the same events must have the same latest_sequence"
        );
    }
}
