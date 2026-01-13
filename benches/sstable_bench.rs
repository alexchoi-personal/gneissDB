use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use gneissdb::{Db, Options};
use tempfile::tempdir;

fn sstable_flush(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("sstable_flush");
    for size in [100, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let options = Options::default().memtable_size(1024 * 1024);
                    let db = rt.block_on(Db::open(dir.path(), options)).unwrap();
                    rt.block_on(async {
                        for i in 0..size {
                            let key = format!("key{:08}", i);
                            let value = "x".repeat(100);
                            db.put(key, value).await.unwrap();
                        }
                    });
                    (dir, db)
                },
                |(_dir, db)| {
                    rt.block_on(async {
                        db.flush().await.unwrap();
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn sstable_read_after_flush(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("sstable_read");
    for size in [100, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let options = Options::default().memtable_size(1024 * 1024);
                    let db = rt.block_on(Db::open(dir.path(), options)).unwrap();
                    rt.block_on(async {
                        for i in 0..size {
                            let key = format!("key{:08}", i);
                            let value = "x".repeat(100);
                            db.put(key, value).await.unwrap();
                        }
                        db.flush().await.unwrap();
                    });
                    (dir, db, size)
                },
                |(_dir, db, size)| {
                    rt.block_on(async {
                        for i in 0..size {
                            let key = format!("key{:08}", i);
                            let _ = db.get(&key).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn sstable_compaction(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("sstable_compaction");
    group.sample_size(10);
    group.bench_function("compact_l0", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let options = Options::default().memtable_size(4096);
                let db = rt.block_on(Db::open(dir.path(), options)).unwrap();
                rt.block_on(async {
                    for i in 0..500 {
                        let key = format!("key{:08}", i);
                        let value = "x".repeat(100);
                        db.put(key, value).await.unwrap();
                    }
                });
                (dir, db)
            },
            |(_dir, db)| {
                rt.block_on(async {
                    db.compact().await.unwrap();
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    sstable_flush,
    sstable_read_after_flush,
    sstable_compaction
);
criterion_main!(benches);
