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

/// Bounded output capture for adapter/CLI processes, including malformed or hung peers.
pub fn output(mut command: Command, input: Option<&[u8]>) -> std::process::Output {
    use std::io::{Read, Write};
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = Process(command.spawn().expect("spawn transport peer"));
    if let Some(input) = input {
        child.0.stdin.take().unwrap().write_all(input).unwrap();
    } else {
        child.0.stdin.take();
    }
    let stdout = child.0.stdout.take().unwrap();
    let stderr = child.0.stderr.take().unwrap();
    let out = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.take(4 * 1024 * 1024).read_to_end(&mut bytes).unwrap();
        bytes
    });
    let err = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.take(1024 * 1024).read_to_end(&mut bytes).unwrap();
        bytes
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(35);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break Some(status);
        }
        if std::time::Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    if status.is_none() {
        let _ = child.0.kill();
        let _ = child.0.wait();
    }
    let stdout = out.join().unwrap();
    let stderr = err.join().unwrap();
    std::process::Output {
        status: status.expect("transport peer exceeded 35 seconds"),
        stdout,
        stderr,
    }
}
