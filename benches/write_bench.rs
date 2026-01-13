use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use gneissdb::{Db, Options, WriteBatch, WriteOptions};
use tempfile::tempdir;

fn batch_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("batch_write");
    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let db = rt
                            .block_on(Db::open(dir.path(), Options::default()))
                            .unwrap();
                        (dir, db, batch_size)
                    },
                    |(_dir, db, batch_size)| {
                        rt.block_on(async {
                            let mut batch = WriteBatch::new();
                            for i in 0..batch_size {
                                batch.put(format!("key{:08}", i), format!("value{:08}", i));
                            }
                            db.write(batch).await.unwrap();
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn sequential_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("sequential_writes");
    for size in [100, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = rt
                        .block_on(Db::open(dir.path(), Options::default()))
                        .unwrap();
                    (dir, db, size)
                },
                |(_dir, db, size)| {
                    rt.block_on(async {
                        for i in 0..size {
                            let key = format!("key{:08}", i);
                            let value = format!("value{:08}", i);
                            db.put(key, value).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn write_with_sync(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("write_with_sync");
    group.sample_size(20);
    for sync in [false, true] {
        let label = if sync { "sync" } else { "async" };
        group.bench_with_input(BenchmarkId::from_parameter(label), &sync, |b, &sync| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = rt
                        .block_on(Db::open(dir.path(), Options::default()))
                        .unwrap();
                    (dir, db, sync)
                },
                |(_dir, db, sync)| {
                    rt.block_on(async {
                        let options = WriteOptions::default().sync(sync);
                        for i in 0..100 {
                            let key = format!("key{:08}", i);
                            let value = format!("value{:08}", i);
                            db.put_opt(key, value, options.clone()).await.unwrap();
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn large_value_writes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("large_value_writes");
    for value_size in [1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(value_size as u64 * 100));
        group.bench_with_input(
            BenchmarkId::from_parameter(value_size),
            &value_size,
            |b, &value_size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let db = rt
                            .block_on(Db::open(dir.path(), Options::default()))
                            .unwrap();
                        let value = "x".repeat(value_size);
                        (dir, db, value)
                    },
                    |(_dir, db, value)| {
                        rt.block_on(async {
                            for i in 0..100 {
                                let key = format!("key{:08}", i);
                                db.put(key, value.clone()).await.unwrap();
                            }
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    batch_write,
    sequential_writes,
    write_with_sync,
    large_value_writes
);
criterion_main!(benches);
