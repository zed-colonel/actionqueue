//! Version-one disposition payload. A bounded JSON typed record is independent of
//! enum discriminants in postcard and retains the domain's validated decoding.
use super::codec::DecodeError;
use crate::mutation::disposition::DispositionRecord;
pub fn decode(bytes: &[u8]) -> Result<DispositionRecord, DecodeError> {
    if bytes.len() + super::codec::HEADER_LEN > actionqueue_core::limits::MAX_ADMISSION_RECORD_BYTES
    {
        return Err(DecodeError::InvalidLength("disposition exceeds format ceiling".into()));
    }
    let bad = |e: serde_json::Error| DecodeError::Decode(e.to_string());
    let mut value: serde_json::Value = serde_json::from_slice(bytes).map_err(bad)?;
    if let Some(children) = value.get_mut("children").and_then(|v| v.as_array_mut()) {
        for child in children {
            let old: super::admission_v2::AdmissionRecordV2 =
                serde_json::from_value(child["admission"].take()).map_err(bad)?;
            let record: crate::mutation::admission::AdmissionRecord = old.try_into()?;
            child["admission"] = serde_json::to_value(record).map_err(bad)?;
        }
    }
    if let Some(wait) =
        value.get_mut("disposition").and_then(|d| d.get_mut("wait")).filter(|w| !w.is_null())
    {
        let object =
            wait.as_object_mut().ok_or_else(|| DecodeError::Decode("invalid v1 wait".into()))?;
        if object.contains_key("target") {
            return Err(DecodeError::Decode("child waits require disposition schema 2".into()));
        }
        let mut target = serde_json::Map::new();
        for key in ["filter", "match_policy", "eligible_from"] {
            target.insert(
                key.into(),
                object.remove(key).ok_or_else(|| DecodeError::Decode("invalid v1 wait".into()))?,
            );
        }
        object.insert("target".into(), serde_json::json!({ "Signal": target }));
    }
    let record: DispositionRecord = serde_json::from_value(value).map_err(bad)?;
    if record.disposition.child_admissions().iter().any(|c| {
        c.task_spec().child_lifecycle_policy()
            != actionqueue_core::task::task_spec::ChildLifecyclePolicy::Required
    }) {
        return Err(DecodeError::Decode("detached children require disposition schema 2".into()));
    }
    Ok(record)
}
