use std::time::Instant;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use anyhow::Error;
use tokio::fs::File;


#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Error> {

    let start = Instant::now();
    let mut i = 0;
    let f = File::open("/Users/guillaume.mazollier/Downloads/slowquery-eu/output_000000.sql").await?;
    const CAP: usize = 1024 * 2048;
    let my_buf_read = BufReader::with_capacity(CAP, f);
    let mut lines = my_buf_read.lines();

    while let Some(line) = lines.next_line().await? {
        i+=1;
        println!("Query = {}", line.len())
    }
    let duration = start.elapsed();
    println!("***");
    println!("Time elapsed: {}µs", duration.as_micros());
    println!("Number of lines {i}");
    Ok(())
}
