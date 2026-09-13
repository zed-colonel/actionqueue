//! Reference adapter for the published one-request JSON protocol.
#[path = "../../../tests/conformance/harness/engine.rs"]
mod engine;
#[path = "../../../tests/conformance/harness/process.rs"]
mod process;
#[path = "../../../tests/conformance/harness/public.rs"]
mod public;
use std::io::Read;
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let mut input = String::new();
    if args.len() == 3 && args[1] == "--request-file" {
        input = std::fs::read_to_string(&args[2]).unwrap();
    } else {
        std::io::stdin().read_to_string(&mut input).unwrap();
    }
    let v: serde_json::Value = serde_json::from_str(&input).unwrap();
    assert_eq!(v["schema_version"], 1);
    let path = std::path::Path::new(v["store"].as_str().unwrap());
    let mode = v["mode"].as_str().unwrap();
    assert!(["early", "late", "deadline", "cancel", "admission", "fanout"].contains(&mode));
    let mut d = public::Driver::new(path, mode, "embedded");
    match v["phase"].as_str().unwrap() {
        "prepare" => d.prepare(),
        "finish" => d.finish(),
        _ => panic!("unsupported phase"),
    };
    let evidence = public::observe(&d.projection());
    if v["hold"] == true {
        std::fs::write(
            path.with_extension("evidence.json"),
            serde_json::to_vec(&evidence).unwrap(),
        )
        .unwrap();
        process::halt("AQ_PUBLIC_PREPARED");
    }
    d.close();
    println!("{}", evidence);
}
