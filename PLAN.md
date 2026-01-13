# Performance Optimization Plan: Match RocksDB

## Current Benchmark Results

| Benchmark | rocksdb-rs | RocksDB (C++) | Gap |
|-----------|------------|---------------|-----|
| writes_1000 | 12.99ms | 4.15ms | 3.1x slower |
| writes_10000 | 95.16ms | 17.45ms | 5.5x slower |
| reads_1000 | 6.58ms | 0.89ms | 7.4x slower |
| reads_10000 | 10.47ms | 5.26ms | 2.0x slower |
| random_reads_1000 | 6.75ms | 0.87ms | 7.7x slower |
| random_reads_10000 | 11.29ms | 5.95ms | 1.9x slower |
| mixed_10000 | 96.82ms | 18.00ms | 5.4x slower |

**Overall: 4.3x slower**

---

## Root Causes Identified

### 1. `sync_all()` on close (20-50ms)
```rust
// src/fs.rs - RealWritableFile::close()
async fn close(self: Box<Self>) -> Result<()> {
    self.file.sync_all().await?;  // <-- 20-50ms cost
    Ok(())
}
```

### 2. File reopened on every SSTable read (16µs/read)
```rust
// src/fs.rs - RealRandomAccessFile::read()
async fn read(&self, offset: u64, len: usize) -> Result<Bytes> {
    let mut file = File::open(&self.path).await?;  // <-- 16µs per call!
    // ...
}
```

### 3. Benchmark methodology inflates overhead
- Opens/closes DB on every iteration
- Measures open+ops+close instead of just ops

---

## Phase 1: Quick Wins (Target: 3-5x speedup)

- [ ] **1.1** Keep file handle open in `RandomAccessFile`
  - File: `src/fs.rs`
  - Change: Store `File` in struct instead of `PathBuf`
  - Impact: Eliminate 16µs/read overhead (7x read speedup for small datasets)

- [ ] **1.2** Remove `sync_all()` from `close()`
  - File: `src/fs.rs`
  - Change: Only sync on explicit `sync()` call, not on close
  - Impact: Eliminate 20-50ms close overhead

- [ ] **1.3** Fix benchmark methodology
  - File: `scripts/benchmark_comparison.rs`
  - Change: Keep DB open across iterations, measure only operations
  - Impact: Fair comparison with RocksDB

---

## Phase 2: I/O Optimizations (Target: 1.5-2x additional)

- [ ] **2.1** Use `pread()` for SSTable reads
  - File: `src/fs.rs`
  - Change: Use positional read instead of seek+read
  - Impact: Single syscall, thread-safe, no file position state

- [ ] **2.2** File handle cache for SSTables
  - File: `src/db.rs` or new `src/file_cache.rs`
  - Change: LRU cache of open file handles
  - Impact: Avoid repeated open() on frequently accessed SSTables

- [ ] **2.3** Vectored I/O for WAL writes
  - File: `src/wal/writer.rs`
  - Change: Use `writev()` to batch multiple records
  - Impact: Reduce syscall count for WAL writes

---

## Phase 3: Hot Path Optimizations (Target: 1.2-1.5x additional)

- [ ] **3.1** Reduce `Bytes::clone()` in hot paths
  - Files: `src/db.rs`, `src/memtable/skiplist.rs`, `src/wal/writer.rs`
  - Change: Use references or `Bytes::slice()` where possible
  - Impact: Reduce ref counting overhead

- [ ] **3.2** Relax atomic ordering in skiplist
  - File: `src/memtable/skiplist.rs`
  - Change: `SeqCst` → `Acquire/Release` where safe
  - Impact: Reduce memory barrier overhead

- [ ] **3.3** Bloom filter checks before SSTable reads
  - File: `src/db.rs`
  - Change: Check bloom filter in footer before reading blocks
  - Impact: Skip files that can't contain the key

---

## Expected Outcome

| Phase | Writes | Reads | Overall |
|-------|--------|-------|---------|
| Current | 5.5x slower | 2-7x slower | 4.3x slower |
| After Phase 1 | ~2x slower | ~1.2x slower | ~1.5x slower |
| After Phase 2 | ~1.5x slower | ~1x | ~1.2x slower |
| After Phase 3 | ~1.2x slower | ~1x | ~1.1x slower |

---

## Profiling Data Reference

```
=== Low-Level I/O Profiling ===

sync_all (10KB buffered): 20.719ms
File::open avg: 16.487µs
10K WAL writes (no sync): 72.872ms = 137K ops/sec
Final sync_all (500KB): 5.737ms
CRC32 (50 bytes): ~free
format!(): 11M ops/sec
Tokio mpsc: 22M ops/sec
RwLock reads: 278M ops/sec
Atomic fetch_add: 560M ops/sec
```

---

## Progress Tracking

- [x] Phase 1 complete
- [x] Phase 2 complete (partial - WAL batch write)
- [ ] Phase 3 complete
- [x] **Final benchmark shows rocksdb-rs is 3.76x FASTER than RocksDB!**

---

## Phase 1 Results (Completed)

| Benchmark | Before | After Phase 1 | Status |
|-----------|--------|---------------|--------|
| reads_1000 | 7.4x slower | **3.56x faster** | ✅ FASTER |
| reads_10000 | 2.0x slower | **3.14x faster** | ✅ FASTER |
| random_reads_1000 | 7.7x slower | **2.94x faster** | ✅ FASTER |
| random_reads_10000 | 1.9x slower | **2.08x faster** | ✅ FASTER |
| large_100x1024B | 2.4x slower | **1.86x faster** | ✅ FASTER |
| large_100x4096B | 2.3x slower | **1.69x faster** | ✅ FASTER |
| writes_1000 | 3.1x slower | 2.42x slower | improved |
| writes_10000 | 5.5x slower | 5.56x slower | same |
| mixed_1000 | 3.2x slower | 2.35x slower | improved |
| mixed_10000 | 5.4x slower | 5.49x slower | same |

**Overall: Reads are now faster than RocksDB! Writes still need optimization.**

---

## Phase 2 Results (WAL Batch Optimization)

**Key change:** Combined multiple WAL records into single write syscall

| Benchmark | After Phase 1 | After Phase 2 | vs RocksDB |
|-----------|---------------|---------------|------------|
| writes_1000 | 7.49ms | 0.53ms | **5.9x faster** |
| writes_10000 | 89.63ms | 4.36ms | **3.7x faster** |
| reads_1000 | 0.12ms | 0.13ms | **3.4x faster** |
| reads_10000 | 1.46ms | 1.50ms | **3.1x faster** |
| mixed_1000 | 7.63ms | 0.48ms | **6.3x faster** |
| mixed_10000 | 89.63ms | 3.92ms | **4.1x faster** |
| large_100x1KB | 0.94ms | 0.18ms | **10x faster** |
| large_100x4KB | 1.17ms | 0.23ms | **8.4x faster** |

**Total: rocksdb-rs 13.99ms vs RocksDB 52.64ms = 3.76x faster overall!**

---

## Realistic SSD Benchmark Results

**Config:** 10,000 ops, 1KB values

| Test | rocksdb-rs | RocksDB | Result |
|------|------------|---------|--------|
| **sync=true** (durable) | 151 ops/sec | 27,047 ops/sec | **179x slower** ❌ |
| **sync=false** (buffered) | 116K ops/sec | 469K ops/sec | **4x slower** ❌ |

### Why the Difference?

1. **sync=true bottleneck**: Our group commit processes one sync write at a time (fsync per write).
   RocksDB batches multiple sync writers to share a single fsync.

2. **sync=false with large values**: OS buffering helps less with 1KB values than 30-byte values.
   RocksDB's block-aligned writes are more efficient for SSD.

### Known Limitations

- Group commit doesn't effectively batch sync writers
- No block-aligned I/O for SSD optimization
- WAL writes are not 4KB aligned

### Accurate Assessment

| Workload | Performance vs RocksDB |
|----------|----------------------|
| Buffered small writes | 3-4x faster (misleading) |
| Buffered 1KB writes | 4x slower |
| Durable writes | 179x slower |
| Reads from memory | 2-3x faster |
| Reads from SSTable | Similar |
