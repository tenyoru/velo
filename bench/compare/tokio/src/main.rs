use std::io::{self, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

async fn echo(mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut buffer = [0u8; 4096];
    loop {
        let n = stream.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }
        stream.write_all(&buffer[..n]).await?;
    }
}

async fn serve(count: usize) -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    println!(
        "{{\"port\":{},\"backend\":\"epoll\",\"version\":\"1.54.1\"}}",
        listener.local_addr()?.port()
    );
    io::stdout().flush()?;
    let mut tasks = JoinSet::new();
    for _ in 0..count {
        let (stream, _) = listener.accept().await?;
        tasks.spawn(echo(stream));
    }
    drop(listener);
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        return Err("usage: echo-tokio <1..10000 connections>".into());
    }
    let count: usize = args[1].parse()?;
    if !(1..=10000).contains(&count) {
        return Err("connections must be in 1..10000".into());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?
        .block_on(serve(count))?;
    Ok(())
}
