use std::collections::BTreeMap;
use std::env;
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use anyhow::Error;

use futures::future::join_all;
use sqlx::Executor;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::fs::File;
use tokio::time::{Instant, Duration};

use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;

struct DistributionMap(Arc<Mutex<BTreeMap<u64, u64>>>);

impl Display for DistributionMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // At this point k is an integer in seconds
        let display = self.0.lock().unwrap().iter().map(|(k, v)| {
            let lower_bound = k;
            let higher_bound = k + 1;
            format!("{lower_bound}:{v}")
        })
            .collect::<Vec<String>>()
            .join(";");
        write!(f, "{display}")
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() -> Result<(), Error> {

    let start = Instant::now();
    let mysql_pool = MySqlPoolOptions::new()
        .max_connections(200)
        .acquire_timeout(Duration::from_secs(600))
        .connect(&env::var("DATABASE_URL")?).await?;
    let pg_pool = PgPoolOptions::new()
        .max_connections(200)
        .acquire_timeout(Duration::from_secs(600))
        .connect(&env::var("PG_URL")?).await?;
    const CAP: usize = 1024 * 4096;
    let filename = env::args().nth(1).expect("You need to provide the filename as arg1");
    let f = File::open(filename).await?;
    let my_buf_read = BufReader::with_capacity(CAP, f);
    let mut lines = my_buf_read.lines();

    let mut i = 0;
    let result_distribution: DistributionMap = DistributionMap(Arc::new(Mutex::new(BTreeMap::new())));
    let err_counter = Arc::new(AtomicUsize::new(0));
    let mut tasks = vec![];
    while let Some(line) = lines.next_line().await? {
        let mut pg_conn = pg_pool.acquire().await?;
        let mut mysql_conn = mysql_pool.acquire().await?;
        i+=1;
        let err_counter = err_counter.clone();
        let local_distribution = result_distribution.0.clone();
        tasks.push(tokio::spawn(async move {
            let pg_start = Instant::now();
            let response_pg = pg_conn.fetch_all(line.as_str()).await;
            let pg_duration = pg_start.elapsed();
            let mysql_start = Instant::now();
            let response_mysql = mysql_conn.fetch_all(line.as_str()).await;
            let mysql_duration = mysql_start.elapsed();

            /*if pg_duration.as_millis() > 5000 {
                println!("{i}: duration: {}ms", pg_duration.as_millis());
                println!("***");
                println!("{line}");
                println!("***");
            }*/
            let key = (pg_duration.as_millis() / 10) as u64;
            let mut distribution = local_distribution.lock().unwrap();
            let count = *distribution.get(&key).unwrap_or_else(|| &0);
            distribution.insert(key, count + 1);

            let mut pg_len = 0;
            let mut mysql_len = 0;
            if let Ok(result) = response_pg {
                pg_len = result.len();
            } else {
                // println!("{i}: {e}");
                err_counter.fetch_add(1, Ordering::SeqCst);
            }
            if let Ok(result) = response_mysql {
                mysql_len = result.len();
            } else {
                // println!("{i}: {e}");
                err_counter.fetch_add(1, Ordering::SeqCst);
            }

            if pg_len != mysql_len {
                println!("***");
                println!("Nb of lines: PG {pg_len} vs MYSQL {mysql_len}");
                println!("{line}");
                println!("***");
            }

        }));
    }
    let _results = join_all(tasks).await; // Await in parallel for all the tasks pushed in the vec

    let duration = start.elapsed();
    println!("Time elapsed: {}µs", duration.as_micros());
    println!("Number of lines {i}");
    println!("Req/s {}", i / duration.as_secs() + 1);
    println!("Number of responses err {:?}", err_counter);
    println!("Response time distribution {}", result_distribution);
    println!("***");
    Ok(())
}