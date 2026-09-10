//! Thin adapters over storage-owned offline operations.
use crate::{
    args::StorageArgs,
    cmd::{CliError, CommandOutput},
};
pub fn run(args: StorageArgs) -> Result<CommandOutput, CliError> {
    use actionqueue_storage::store::*;
    let encode = |v| serde_json::to_value(v).map_err(|e| StoreError::InvalidStore(e.to_string()));
    let result = match args.operation.as_str() {
        "inspect" => inspect_store(&args.data_dir).and_then(|v| {
            serde_json::to_value(v).map_err(|e| StoreError::InvalidStore(e.to_string()))
        }),
        "backup" => backup_store(&args.data_dir, args.output.as_deref().expect("validated output"))
            .and_then(encode),
        "restore" => restore_store(args.input.as_deref().expect("validated input"), &args.data_dir)
            .and_then(encode),
        _ => return Err(CliError::usage("invalid_storage_operation", "unknown storage operation")),
    }
    .map_err(|e| {
        CliError::runtime(
            match e {
                StoreError::StoreInUse => "store_in_use",
                StoreError::MissingTargetManifest => "missing_target_manifest",
                StoreError::UnsupportedStoreFormat { .. } => "unsupported_store_format",
                StoreError::UnsupportedFeatures(_) => "unsupported_features",
                _ => "invalid_store",
            },
            e.to_string(),
        )
    })?;
    if args.json {
        Ok(CommandOutput::Json(result))
    } else {
        Ok(CommandOutput::Text(serde_json::to_string_pretty(&result).expect("JSON value")))
    }
}
