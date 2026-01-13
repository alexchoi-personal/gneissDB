# GneissDB

A high-performance, LSM-tree based key-value storage engine written in Rust, inspired by RocksDB.

## Features

- **LSM-Tree Architecture**: Log-structured merge-tree design for write-optimized workloads
- **Async I/O**: Built on Tokio for non-blocking operations
- **Write-Ahead Log (WAL)**: Durability guarantees with configurable sync options
- **SSTable Storage**: Immutable sorted string tables with block-based layout
- **Bloom Filters**: Reduce disk reads for non-existent keys
- **Block Cache**: LRU cache for frequently accessed data blocks
- **Memory-Mapped Files**: Fast SSTable reads via `mmap`
- **Leveled Compaction**: Background compaction to optimize read performance
- **Batch Writes**: Atomic multi-key operations with `WriteBatch`
- **Range Scans**: Efficient ordered iteration over key ranges
- **io_uring Support**: Optional Linux io_uring backend for high-performance I/O

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
gneissdb = "0.1"
```

### Feature Flags

- `io_uring` - Enable io_uring I/O backend (Linux only)
- `custom-memtable` - Use custom arena-based skiplist instead of crossbeam-skiplist

## Usage

```rust
use gneissdb::{Db, Options, WriteBatch};

#[tokio::main]
async fn main() -> gneissdb::Result<()> {
    // Open database
    let db = Db::open("my_db", Options::default()).await?;

    // Basic operations
    db.put("key", "value").await?;
    
    if let Some(value) = db.get("key").await? {
        println!("Got: {:?}", value);
    }

    db.delete("key").await?;

    // Batch writes
    let mut batch = WriteBatch::new();
    batch.put("k1", "v1");
    batch.put("k2", "v2");
    batch.delete("k3");
    db.write(batch).await?;

    // Range scan
    let results = db.scan("a", "z", 100);
    for (key, value) in results {
        println!("{:?} -> {:?}", key, value);
    }

    // Flush memtable to disk
    db.flush().await?;

    // Trigger compaction
    db.compact().await?;

    db.close().await?;
    Ok(())
}
```

## Configuration

```rust
use gneissdb::{Options, WriteOptions, ReadOptions, IoEngine};

let options = Options::default()
    .memtable_size(64 * 1024 * 1024)  // 64MB memtable
    .max_levels(7)
    .block_size(4096)
    .bloom_bits_per_key(10)
    .block_cache_size(128 * 1024 * 1024)  // 128MB block cache
    .sync_writes(false);  // Disable sync by default

// Per-operation options
let write_opts = WriteOptions { sync: true };  // Force sync for this write
let read_opts = ReadOptions::default();
```

## Architecture

```
┌────────────────────────────────────────────────┐
│                   GneissDB                     │
├────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  MemTable   │  │   Write-Ahead Log       │  │
│  │ (SkipList)  │  │        (WAL)            │  │
│  └──────┬──────┘  └─────────────────────────┘  │
│         │ flush                                │
│         ▼                                      │
│  ┌─────────────────────────────────────────┐   │
│  │            SSTable Files                │   │
│  │  ┌───────┬───────┬───────┬───────────┐  │   │
│  │  │ Data  │ Index │ Bloom │  Footer   │  │   │
│  │  │Blocks │ Block │Filter │           │  │   │
│  │  └───────┴───────┴───────┴───────────┘  │   │
│  └─────────────────────────────────────────┘   │
│                      │                         │
│  ┌───────────────────┴───────────────────┐     │
│  │         Leveled Compaction            │     │
│  │  L0: [sst] [sst] [sst]                │     │
│  │  L1: [    sst    ] [    sst    ]      │     │
│  │  L2: [        sst        ] ...        │     │
│  └───────────────────────────────────────┘     │
└────────────────────────────────────────────────┘
```

## Development

Set up git hooks:

```bash
git config core.hooksPath .githooks
```

This enables pre-commit checks for formatting and linting.

## Benchmarks

Run benchmarks against RocksDB:

```bash
cargo run --release --example benchmark_comparison_full
```

Run microbenchmarks:

```bash
cargo bench
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
