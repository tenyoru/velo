use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Clone, Copy)]
struct Config {
    tasks: usize,
    iterations: usize,
    capacity: usize,
    work: usize,
    payload: usize,
    active: usize,
    warmup: usize,
}

#[repr(C)]
struct Timespec {
    sec: i64,
    nsec: i64,
}
extern "C" {
    fn clock_gettime(clock: i32, ts: *mut Timespec) -> i32;
}
fn cpu() -> u64 {
    let mut ts = Timespec { sec: 0, nsec: 0 };
    assert_eq!(unsafe { clock_gettime(2, &mut ts) }, 0);
    ts.sec as u64 * 1_000_000_000 + ts.nsec as u64
}
fn command() {
    let mut line = String::new();
    io::stdin().read_line(&mut line).unwrap();
    assert_eq!(line, "\n", "protocol: expected newline");
}
fn phase(name: &str, tasks: usize) {
    println!("{{\"phase\":\"{name}\",\"tasks\":{tasks}}}");
    io::stdout().flush().unwrap();
    command();
}
fn affine(steps: usize) -> (u64, u64) {
    let (mut a, mut b) = (1u64, 0u64);
    for _ in 0..steps {
        a = a.wrapping_mul(1664525);
        b = b.wrapping_mul(1664525).wrapping_add(1013904223);
    }
    (a, b)
}
struct ResultRow {
    operations: usize,
    checksum: u64,
    elapsed: u128,
    cpu: u64,
    setup: u128,
    drain: u128,
    completed: usize,
}
impl ResultRow {
    fn emit(self) {
        println!("{{\"phase\":\"result\",\"operations\":{},\"checksum\":{},\"elapsed_ns\":{},\"cpu_ns\":{},\"setup_ns\":{},\"drain_ns\":{},\"completed\":{}}}",
            self.operations,self.checksum,self.elapsed,self.cpu,self.setup,self.drain,self.completed);
    }
}
async fn sleeper(
    mut input: mpsc::Receiver<u64>,
    ready: mpsc::Sender<()>,
    bytes: Arc<Vec<u8>>,
    id: usize,
    len: usize,
) -> u64 {
    ready.send(()).await.unwrap();
    input.recv().await.unwrap();
    let sum = bytes[id * len..(id + 1) * len]
        .iter()
        .map(|&v| v as u64)
        .sum();
    assert_eq!(sum, ((id % 251) * len) as u64);
    sum
}
async fn execute(kind: &str, c: Config, protocol: bool) -> ResultRow {
    let setup = Instant::now();
    let count = if kind == "pipeline" {
        c.tasks - 1
    } else {
        c.tasks
    };
    let capacity = if kind == "pipeline" { c.capacity } else { 1 };
    let mut tx = Vec::with_capacity(count);
    let mut rx = Vec::with_capacity(count);
    for _ in 0..count {
        let (s, r) = mpsc::channel::<u64>(capacity);
        tx.push(s);
        rx.push(Some(r));
    }
    let (ready_tx, mut ready_rx) = mpsc::channel::<()>(1);
    let (gate_tx, gate_rx) = mpsc::channel::<u64>(1);
    let mut gate_rx = Some(gate_rx);
    let mut handles = Vec::with_capacity(c.tasks);
    let mut bytes = vec![
        0u8;
        if kind == "parked" {
            c.tasks * c.payload
        } else {
            0
        }
    ];
    if kind == "parked" {
        for id in 0..c.tasks {
            bytes[id * c.payload..(id + 1) * c.payload].fill((id % 251) as u8);
        }
    }
    let bytes = Arc::new(bytes);
    let (a, b) = affine(if kind == "pipeline" {
        (c.tasks - 2) * c.work
    } else {
        0
    });
    for id in 0..c.tasks {
        let ready = ready_tx.clone();
        let input = if kind == "pipeline" {
            if id == 0 {
                gate_rx.take().unwrap()
            } else {
                rx[id - 1].take().unwrap()
            }
        } else {
            rx[id].take().unwrap()
        };
        if kind == "parked" {
            handles.push(Some(tokio::spawn(sleeper(
                input,
                ready,
                bytes.clone(),
                id,
                c.payload,
            ))));
            continue;
        }
        let output = if kind == "handoff" {
            Some(tx[(id + 1) % c.tasks].clone())
        } else if kind == "pipeline" && id < c.tasks - 1 {
            Some(tx[id].clone())
        } else {
            None
        };
        let kind = if kind == "handoff" {
            "handoff"
        } else {
            "pipeline"
        };
        handles.push(Some(tokio::spawn(async move {
            let mut input = input;
            ready.send(()).await.unwrap();
            let mut checksum = 0u64;
            match kind {
                "handoff" => {
                    for k in 0..c.iterations {
                        let value = input.recv().await.unwrap();
                        assert_eq!(value, (k * c.tasks + id) as u64);
                        if id == c.tasks - 1 && k == c.iterations - 1 {
                            checksum = value + 1;
                        } else {
                            output.as_ref().unwrap().send(value + 1).await.unwrap();
                        }
                    }
                }
                "pipeline" => {
                    if id == 0 {
                        input.recv().await.unwrap();
                        for k in 0..c.iterations {
                            output.as_ref().unwrap().send(k as u64).await.unwrap();
                        }
                    } else {
                        let mut expected = b;
                        for _ in 0..c.iterations {
                            let mut value = input.recv().await.unwrap();
                            if id == c.tasks - 1 {
                                assert_eq!(value, expected);
                                expected = expected.wrapping_add(a);
                                checksum = checksum.wrapping_add(value);
                            } else {
                                for _ in 0..c.work {
                                    value = value.wrapping_mul(1664525).wrapping_add(1013904223);
                                }
                                output.as_ref().unwrap().send(value).await.unwrap();
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            checksum
        })));
    }
    for _ in 0..c.tasks {
        ready_rx.recv().await.unwrap();
    }
    tokio::task::yield_now().await;
    let setup_ns = setup.elapsed().as_nanos();
    if protocol {
        phase("ready", c.tasks);
    }
    let cpu_start = cpu();
    let begin = Instant::now();
    let mut checksum = 0u64;
    let operations;
    if kind == "parked" {
        operations = c.active;
        for i in 0..c.active {
            let id = i * c.tasks / c.active;
            tx[id].send(0).await.unwrap();
            checksum = checksum.wrapping_add(handles[id].take().unwrap().await.unwrap());
        }
    } else {
        operations = if kind == "handoff" {
            c.tasks * c.iterations
        } else {
            c.iterations
        };
        if kind == "handoff" {
            tx[0].send(0).await.unwrap();
        } else {
            gate_tx.send(0).await.unwrap();
        }
        for (id, handle) in handles.iter_mut().enumerate() {
            let value = handle.take().unwrap().await.unwrap();
            if id == c.tasks - 1 {
                checksum = value;
            }
        }
    }
    let elapsed = begin.elapsed().as_nanos();
    let cpu_ns = cpu() - cpu_start;
    let mut drain = 0;
    if kind == "parked" {
        let begin = Instant::now();
        for (id, handle) in handles.iter_mut().enumerate() {
            if let Some(handle) = handle.take() {
                tx[id].send(0).await.unwrap();
                handle.await.unwrap();
            }
        }
        drain = begin.elapsed().as_nanos();
    }
    ResultRow {
        operations,
        checksum,
        elapsed,
        cpu: cpu_ns,
        setup: setup_ns,
        drain,
        completed: c.tasks,
    }
}
fn main() {
    assert!(
        cfg!(all(target_os = "linux", target_pointer_width = "64")),
        "64-bit Linux only"
    );
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(
        args.len(),
        8,
        "usage: workload tasks iterations capacity work payload active warmup"
    );
    let kind = &args[0];
    assert!(["handoff", "pipeline", "parked"].contains(&kind.as_str()));
    let n: Vec<usize> = args[1..].iter().map(|s| s.parse().unwrap()).collect();
    let c = Config {
        tasks: n[0],
        iterations: n[1],
        capacity: n[2],
        work: n[3],
        payload: n[4],
        active: n[5],
        warmup: n[6],
    };
    assert!(
        (2..=100000).contains(&c.tasks)
            && (1..=10000000).contains(&c.iterations)
            && (1..=65536).contains(&c.capacity)
            && c.work <= 10000
            && c.payload <= 65536
            && c.active > 0
            && c.active <= c.tasks
            && c.warmup <= 1000000
    );
    assert!(
        c.tasks * c.payload <= 256 * 1024 * 1024
            && c.tasks * c.capacity <= 10000000
            && (kind != "pipeline" || c.tasks >= 3)
            && (kind != "parked" || (c.iterations == 1 && c.warmup == 0))
            && c.tasks * c.iterations.max(c.warmup) * c.work.max(1) <= 100000000
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        if kind != "parked" && c.warmup != 0 {
            execute(
                kind,
                Config {
                    iterations: c.warmup,
                    ..c
                },
                false,
            )
            .await;
        }
        phase("baseline", 0);
        execute(kind, c, true).await.emit();
    });
}
