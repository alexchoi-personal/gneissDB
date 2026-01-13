use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rocksdb_rs::{Db, Options};
use tempfile::tempdir;

fn memtable_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memtable_insert");
    for size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = rt.block_on(Db::open(dir.path(), Options::default())).unwrap();
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

fn memtable_lookup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memtable_lookup");
    for size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = rt.block_on(Db::open(dir.path(), Options::default())).unwrap();
                    rt.block_on(async {
                        for i in 0..size {
                            let key = format!("key{:08}", i);
                            let value = format!("value{:08}", i);
                            db.put(key, value).await.unwrap();
                        }
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

fn memtable_mixed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("memtable_mixed");
    group.throughput(Throughput::Elements(1000));
    group.bench_function("50_50_read_write", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = rt.block_on(Db::open(dir.path(), Options::default())).unwrap();
                rt.block_on(async {
                    for i in 0..500 {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(key, value).await.unwrap();
                    }
                });
                (dir, db)
            },
            |(_dir, db)| {
                rt.block_on(async {
                    for i in 500..1000 {
                        let key = format!("key{:08}", i);
                        let value = format!("value{:08}", i);
                        db.put(&key, value).await.unwrap();

                        let read_key = format!("key{:08}", i - 500);
                        let _ = db.get(&read_key).await.unwrap();
                    }
                });
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, memtable_insert, memtable_lookup, memtable_mixed);
criterion_main!(benches);
