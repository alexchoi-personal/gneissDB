use std::time::Instant;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() {
    println!("=== Low-Level I/O Profiling ===\n");

    let dir = tempdir().unwrap();
    let path = dir.path().join("test.dat");
    let data = vec![0u8; 100];

    println!("--- File sync_all cost ---");
    {
        let mut file = File::create(&path).await.unwrap();
        for _ in 0..100 {
            file.write_all(&data).await.unwrap();
        }

        let start = Instant::now();
        file.sync_all().await.unwrap();
        println!(
            "sync_all (10KB buffered): {:.3}ms",
            start.elapsed().as_secs_f64() * 1000.0
        );
    }

    println!("\n--- File open cost ---");
    {
        let mut times = Vec::new();
        for _ in 0..100 {
            let start = Instant::now();
            let _file = File::open(&path).await.unwrap();
            times.push(start.elapsed());
        }
        let avg: std::time::Duration = times.iter().sum::<std::time::Duration>() / 100;
        println!("File::open avg: {:.3}µs", avg.as_secs_f64() * 1_000_000.0);
    }

    println!("\n--- WAL-like sequential writes ---");
    {
        let path2 = dir.path().join("wal.dat");
        let mut file = File::create(&path2).await.unwrap();
        let record = vec![0u8; 50]; // ~50 byte record

        let start = Instant::now();
        for _ in 0..10000 {
            file.write_all(&record).await.unwrap();
        }
        let write_time = start.elapsed();
        println!(
            "10K writes (no sync): {:.3}ms = {:.0} ops/sec",
            write_time.as_secs_f64() * 1000.0,
            10000.0 / write_time.as_secs_f64()
        );

        let start = Instant::now();
        file.sync_all().await.unwrap();
        println!(
            "Final sync_all (500KB): {:.3}ms",
            start.elapsed().as_secs_f64() * 1000.0
        );
    }

    println!("\n--- CRC32 computation cost ---");
    {
        let data = vec![0u8; 50];
        let start = Instant::now();
        for _ in 0..100000 {
            let _ = crc32fast::hash(&data);
        }
        let crc_time = start.elapsed();
        println!(
            "100K CRC32 (50 bytes each): {:.3}ms = {:.0} ops/sec",
            crc_time.as_secs_f64() * 1000.0,
            100000.0 / crc_time.as_secs_f64()
        );
    }

    println!("\n--- String formatting cost ---");
    {
        let start = Instant::now();
        for i in 0..100000 {
            let _key = format!("key{:08}", i);
            let _value = format!("value{:08}", i);
        }
        let fmt_time = start.elapsed();
        println!(
            "100K format! calls: {:.3}ms = {:.0} ops/sec",
            fmt_time.as_secs_f64() * 1000.0,
            100000.0 / fmt_time.as_secs_f64()
        );
    }

    println!("\n--- Tokio channel overhead ---");
    {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(4096);

        let sender = tokio::spawn(async move {
            let start = Instant::now();
            for i in 0..100000 {
                tx.send(i).await.unwrap();
            }
            start.elapsed()
        });

        let receiver = tokio::spawn(async move {
            let mut count = 0u64;
            while rx.recv().await.is_some() {
                count += 1;
                if count >= 100000 {
                    break;
                }
            }
        });

        let send_time = sender.await.unwrap();
        receiver.await.unwrap();
        println!(
            "100K mpsc sends: {:.3}ms = {:.0} ops/sec",
            send_time.as_secs_f64() * 1000.0,
            100000.0 / send_time.as_secs_f64()
        );
    }

    println!("\n--- Oneshot channel overhead ---");
    {
        let start = Instant::now();
        for _ in 0..100000 {
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            tx.send(()).unwrap();
            rx.await.unwrap();
        }
        let oneshot_time = start.elapsed();
        println!(
            "100K oneshot round-trips: {:.3}ms = {:.0} ops/sec",
            oneshot_time.as_secs_f64() * 1000.0,
            100000.0 / oneshot_time.as_secs_f64()
        );
    }

    println!("\n--- RwLock contention ---");
    {
        use parking_lot::RwLock;
        use std::sync::Arc;

        let lock = Arc::new(RwLock::new(0u64));

        let start = Instant::now();
        for _ in 0..100000 {
            let _ = *lock.read();
        }
        let read_time = start.elapsed();

        let start2 = Instant::now();
        for _ in 0..100000 {
            *lock.write() += 1;
        }
        let write_time = start2.elapsed();

        println!(
            "100K RwLock reads:  {:.3}ms = {:.0} ops/sec",
            read_time.as_secs_f64() * 1000.0,
            100000.0 / read_time.as_secs_f64()
        );
        println!(
            "100K RwLock writes: {:.3}ms = {:.0} ops/sec",
            write_time.as_secs_f64() * 1000.0,
            100000.0 / write_time.as_secs_f64()
        );
    }

    println!("\n--- Atomic operations ---");
    {
        use std::sync::atomic::{AtomicU64, Ordering};

        let counter = AtomicU64::new(0);

        let start = Instant::now();
        for _ in 0..1000000 {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        let atomic_time = start.elapsed();
        println!(
            "1M atomic fetch_add: {:.3}ms = {:.0} ops/sec",
            atomic_time.as_secs_f64() * 1000.0,
            1000000.0 / atomic_time.as_secs_f64()
        );
    }
}
