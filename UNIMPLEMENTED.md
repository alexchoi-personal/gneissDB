# GneissDB - Unimplemented Features Audit

## Summary

This document tracks implemented vs unimplemented features in GneissDB.

---

## ✅ IMPLEMENTED (Working)

### Core Operations
- [x] Put/Get/Delete operations
- [x] WriteBatch for atomic batch writes
- [x] Range scan (memtable + immutable memtables + SSTables)
- [x] WAL write and recovery
- [x] Group commit for sync writes

### Storage
- [x] Memtable (crossbeam-skiplist based)
- [x] SSTable format (blocks, index, bloom filter, footer)
- [x] SSTable reader with mmap
- [x] SSTable iterator for sequential access
- [x] Block cache (LRU-ish)
- [x] Table cache

### Durability
- [x] WAL writer with CRC checksums
- [x] WAL reader for recovery
- [x] Crash recovery from WAL
- [x] Manifest persistence (MANIFEST and CURRENT files)

### Compaction
- [x] Compaction merge logic (multi-way merge)
- [x] Duplicate key elimination
- [x] Deletion tombstone handling
- [x] File cleanup (WAL and SST files)

---

## ❌ NOT IMPLEMENTED

### 1. Snapshots (NOT IMPLEMENTED)
**Location:** `src/options.rs`

```rust
pub struct ReadOptions {
    pub(crate) snapshot: Option<u64>,  // Field exists but unused
}
```

**Missing:**
- Snapshot creation API
- Snapshot-consistent reads
- Snapshot reference counting

### 8. Compression (NOT IMPLEMENTED)

SSTable blocks are stored uncompressed:
- No Snappy/LZ4/Zstd support
- No compression option in config

### 9. Bloom Filter False Positive Tuning

Bloom filter exists but:
- No way to disable it
- bits_per_key is hardcoded at build time

### 10. Statistics/Metrics (NOT IMPLEMENTED)

No observability:
- No read/write latency metrics
- No cache hit/miss rates
- No compaction statistics

---

## 🔧 PARTIALLY IMPLEMENTED

### Write Stall
- ✅ L0 stop trigger (hard block)
- ✅ L0 slowdown trigger (delay)
- ❌ No actual relief (compaction doesn't reduce L0 count)

### Block Cache
- ✅ Basic caching works
- ❌ `size()` and `clear()` unused
- ❌ No cache statistics

### Table Cache
- ✅ Caching readers works
- ❌ `evict()` and `clear()` unused
- ❌ No LRU eviction policy

---

## Dead Code Summary

| Location | Dead Code | Reason |
|----------|-----------|--------|
| `compaction/level.rs` | `MergeEntry` struct | Compaction not implemented |
| `compaction/picker.rs` | `needs_compaction()` | Never called |
| `manifest/edit.rs` | `encode()`, `decode()` | Manifest not persisted |
| `manifest/version.rs` | `manifest_file_number` | Manifest not persisted |
| `db.rs` | `write_lock`, `DbBatchOp`, `write_batch()` | Alternative batch path unused |
| `fs.rs` | Multiple trait methods | Not all FS ops needed |
| `sstable/block.rs` | `reset()`, `iter()` | No SSTable iteration |
| `sstable/reader.rs` | `min_key()`, `max_key()`, `file_number()` | Accessors unused |
| `types.rs` | `UserKey`, `UserValue`, `InternalKey` methods | Type aliases/methods unused |
| `wal/writer.rs` | `path`, `append_batch_owned()` | Alternative paths |

---

## Recommended Priority

### P0 - Critical ✅ ALL COMPLETE
1. ~~**Manifest persistence**~~ ✅ Implemented
2. ~~**Compaction merge logic**~~ ✅ Implemented
3. ~~**Old file cleanup**~~ ✅ Implemented

### P1 - Important ✅ MOSTLY COMPLETE
4. ~~**SSTable iterator**~~ ✅ Implemented
5. ~~**Range scan on SSTables**~~ ✅ Implemented
6. **Background compaction** - Currently only manual

### P2 - Nice to Have
7. Compression
8. Snapshots
9. Statistics/Metrics
10. Bloom filter tuning

---

## Estimated Effort

| Feature | Effort | Status |
|---------|--------|--------|
| Manifest persistence | 2-3 days | ✅ Done |
| SSTable iterator | 1-2 days | ✅ Done |
| Compaction merge | 2-3 days | ✅ Done |
| Range scan on SST | 1 day | ✅ Done |
| File cleanup | 0.5 day | ✅ Done |
| Background compaction | 1-2 days | Pending |
| Compression | 1-2 days | Pending |
| Snapshots | 2-3 days | Pending |
