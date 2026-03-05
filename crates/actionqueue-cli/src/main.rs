//! CLI entry point for the ActionQueue system.
//!
//! This binary provides command-line access to ActionQueue functionality:
//! - Start the ActionQueue daemon
//! - Submit tasks for execution
//! - Query system statistics

use actionqueue_cli::args::{parse_args, Command};
use actionqueue_cli::cmd;
use actionqueue_cli::cmd::{CliError, CommandOutput, ErrorPayload};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        emit_error_and_exit(CliError::usage(
            "usage_missing_command",
            "Usage: actionqueue-cli <command> [options]\nAvailable commands: daemon, submit, stats",
        ));
    }

    let parsed = match parse_args(&args[1..]) {
        Ok(command) => command,
        Err(message) => {
            let error = if message.contains("requires a value")
                || message.contains("Unknown option")
            {
                CliError::usage("usage_parse_error", message)
            } else if message.contains("Unknown command") || message.contains("No command provided")
            {
                CliError::usage("usage_invalid_command", message)
            } else {
                CliError::validation("input_validation_failed", message)
            };
            emit_error_and_exit(error);
        }
    };

    let outcome = match parsed {
        Command::Daemon(daemon_args) => cmd::daemon::run(daemon_args),
        Command::Submit(submit_args) => cmd::submit::run(submit_args),
        Command::Stats(stats_args) => cmd::stats::run(stats_args),
    };

    match outcome {
        Ok(output) => emit_success(output),
        Err(error) => emit_error_and_exit(error),
    }
}

fn emit_success(output: CommandOutput) {
    match output {
        CommandOutput::Text(text) => {
            println!("{text}");
        }
        CommandOutput::Json(json_value) => {
            let rendered = serde_json::to_string_pretty(&json_value)
                .expect("serializing command success payload should not fail");
            println!("{rendered}");
        }
    }
}

fn emit_error_and_exit(error: CliError) -> ! {
    let payload = ErrorPayload {
        error_kind: error.kind(),
        error_code: error.code(),
        message: error.message(),
    };
    let rendered =
        serde_json::to_string(&payload).expect("serializing command error payload should not fail");
    eprintln!("{rendered}");
    std::process::exit(error.kind().exit_code());
}
