//! Bounded subprocess ownership: every exit path kills and reaps the child.
#![allow(dead_code)]
use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};
pub struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub fn kill_at(mut command: Command, expected: &str) -> String {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = Process(command.spawn().expect("spawn crash worker"));
    let stdout = child.0.stdout.take().unwrap();
    let stderr = child.0.stderr.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    let errors = std::thread::spawn(move || {
        use std::io::Read;
        let mut s = String::new();
        let _ = BufReader::new(stderr).take(1024 * 1024).read_to_string(&mut s);
        s
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut output = String::new();
    let mut acknowledged = false;
    while let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) {
        let Ok(line) = rx.recv_timeout(remaining) else { break };
        output.push_str(&line);
        output.push('\n');
        if line == expected {
            acknowledged = true;
            break;
        }
        if output.len() > 1024 * 1024 {
            break;
        }
    }
    let _ = child.0.kill();
    child.0.wait().expect("reap crash worker");
    reader.join().expect("stdout reader");
    output.push_str(&errors.join().expect("stderr reader"));
    assert!(acknowledged, "worker missed exact boundary {expected:?}:\n{output}");
    output
}
pub fn halt(marker: &str) -> ! {
    use std::io::Write;
    println!("{marker}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
