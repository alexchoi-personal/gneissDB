use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use gneissdb::{Db, Options};
use tempfile::tempdir;

fn read_from_memtable(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("read_from_memtable");
    for size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = rt
                        .block_on(Db::open(dir.path(), Options::default()))
                        .unwrap();
                    rt.block_on(async {
                        for i in 0..size {
                            db.put(format!("key{:08}", i), format!("value{:08}", i))
                                .await
                                .unwrap();
                        }
                    });
                    (dir, db, size)
                },
                |(_dir, db, size)| {
                    rt.block_on(async {
                        for i in 0..size {
                            let _ = db.get(format!("key{:08}", i)).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn read_from_sstable(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("read_from_sstable");
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
                            db.put(format!("key{:08}", i), "x".repeat(100))
                                .await
                                .unwrap();
                        }
                        db.flush().await.unwrap();
                    });
                    (dir, db, size)
                },
                |(_dir, db, size)| {
                    rt.block_on(async {
                        for i in 0..size {
                            let _ = db.get(format!("key{:08}", i)).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn read_missing_keys(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("read_missing_keys");
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
                            db.put(format!("key{:08}", i), "x".repeat(100))
                                .await
                                .unwrap();
                        }
                        db.flush().await.unwrap();
                    });
                    (dir, db, size)
                },
                |(_dir, db, size)| {
                    rt.block_on(async {
                        for i in 0..size {
                            let _ = db.get(format!("missing{:08}", i)).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn random_read_pattern(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("random_read_pattern");
    group.bench_function("1000_keys", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let options = Options::default().memtable_size(1024 * 1024);
                let db = rt.block_on(Db::open(dir.path(), options)).unwrap();
                rt.block_on(async {
                    for i in 0..1000 {
                        db.put(format!("key{:08}", i), "x".repeat(100))
                            .await
                            .unwrap();
                    }
                    db.flush().await.unwrap();
                });
                let indices: Vec<usize> = (0..1000).map(|i| (i * 7) % 1000).collect();
                (dir, db, indices)
            },
            |(_dir, db, indices)| {
                rt.block_on(async {
                    for i in &indices {
                        let _ = db.get(format!("key{:08}", i)).await.unwrap();
                    }
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    read_from_memtable,
    read_from_sstable,
    read_missing_keys,
    random_read_pattern
);
criterion_main!(benches);
