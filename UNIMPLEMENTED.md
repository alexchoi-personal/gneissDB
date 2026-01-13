# GneissDB - Unimplemented Features Audit

## Summary

GneissDB has a working core but several features are stubbed, incomplete, or missing entirely. This document categorizes all unimplemented functionality.

---

## ✅ IMPLEMENTED (Working)

### Core Operations
- [x] Put/Get/Delete operations
- [x] WriteBatch for atomic batch writes
- [x] Range scan (memtable only)
- [x] WAL write and recovery
- [x] Group commit for sync writes

### Storage
- [x] Memtable (crossbeam-skiplist based)
- [x] SSTable format (blocks, index, bloom filter, footer)
- [x] SSTable reader with mmap
- [x] Block cache (LRU-ish)
- [x] Table cache

### Durability
- [x] WAL writer with CRC checksums
- [x] WAL reader for recovery
- [x] Crash recovery from WAL

---

## ❌ NOT IMPLEMENTED

### 1. Compaction (STUBBED - Critical)
**Location:** `src/compaction/level.rs`

The compaction logic is scaffolded but **does not actually merge data**:
```rust
// Current code creates empty output, doesn't read/merge input files
let _heap: BinaryHeap<MergeEntry> = BinaryHeap::new();  // Never used!
let mut iterators: Vec<_> = Vec::new();
for (idx, _reader) in readers.iter().enumerate() {
    iterators.push(idx);  // Just pushes indices, doesn't iterate
}
let metadata = builder.finish().await?;  // Writes empty SSTable
```

**Missing:**
- SSTable iterator for reading entries
- Multi-way merge algorithm
- Duplicate key elimination (keeping newest)
- Deletion tombstone handling
- Background compaction thread

### 2. Manifest Persistence (NOT IMPLEMENTED)
**Location:** `src/manifest/edit.rs`

Version edits can be encoded/decoded but are **never written to disk**:
```rust
// These methods exist but are never called:
pub(crate) fn encode(&self) -> Bytes { ... }
pub(crate) fn decode(data: &[u8]) -> Option<Self> { ... }
```

**Missing:**
- MANIFEST file writing
- MANIFEST file reading on startup
- CURRENT file pointer
- Version recovery after restart

**Impact:** After restart, GneissDB doesn't know which SST files exist or their levels.

### 3. SSTable Iterator (NOT IMPLEMENTED)
**Location:** `src/sstable/reader.rs`

Only point lookups work. No way to iterate through all entries:
```rust
// Block::iter() exists but is never used
// SstableReader has no iter() method
```

**Missing:**
- `SstableReader::iter()` - iterate all entries
- `SstableReader::seek()` - seek to key
- Merging iterator for multi-SSTable scans

**Impact:** Range scans only work on memtable, compaction can't read SSTable contents.

### 4. Range Scan on SSTables (NOT IMPLEMENTED)
**Location:** `src/db.rs:268`

```rust
pub(crate) fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
    // Only scans memtable!
    let memtable = self.memtable.read().clone();
    memtable.scan_range(start, end, sequence, limit)
}
```

**Missing:**
- Scan immutable memtables
- Scan L0 SSTables
- Scan leveled SSTables with key range filtering
- Merge results across all sources

### 5. Old WAL Cleanup (NOT IMPLEMENTED)
**Location:** `src/db.rs`

WAL files are created but never deleted:
```rust
// After flush, a new WAL is created but old one isn't removed
let wal_path = self.path.join(format!("{:06}.wal", ...));
let wal_file = self.fs.create_file(&wal_path).await?;
// Missing: delete old WAL after successful flush
```

### 6. Old SSTable Cleanup (NOT IMPLEMENTED)

After compaction, input SSTables are logically deleted from the version but **files remain on disk**:
```rust
// VersionEdit tracks deleted files but doesn't delete them
edit.delete_file(task.level, file.file_number);
// Missing: actual file deletion
```

### 7. Snapshots (NOT IMPLEMENTED)
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

### P0 - Critical (Database doesn't work correctly without these)
1. **Manifest persistence** - Without this, DB forgets state on restart
2. **Compaction merge logic** - Without this, disk fills up forever
3. **Old file cleanup** - WAL and SST files never deleted

### P1 - Important (Major functionality gaps)
4. **SSTable iterator** - Needed for compaction and range scans
5. **Range scan on SSTables** - Currently only scans memtable
6. **Background compaction** - Currently only manual

### P2 - Nice to Have
7. Compression
8. Snapshots
9. Statistics/Metrics
10. Bloom filter tuning

---

## Estimated Effort

| Feature | Effort | Dependencies |
|---------|--------|--------------|
| Manifest persistence | 2-3 days | None |
| SSTable iterator | 1-2 days | None |
| Compaction merge | 2-3 days | SSTable iterator |
| Range scan on SST | 1 day | SSTable iterator |
| File cleanup | 0.5 day | None |
| Background compaction | 1-2 days | Compaction merge |
| Compression | 1-2 days | None |
| Snapshots | 2-3 days | None |
