use crate::error::Result;
use crate::manifest::{read_current, write_current, ManifestReader, ManifestWriter, VersionEdit};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Debug)]
pub(crate) struct FileMetadata {
    pub(crate) file_number: u64,
    pub(crate) file_size: u64,
    pub(crate) min_key: Bytes,
    pub(crate) max_key: Bytes,
}

#[derive(Clone)]
pub(crate) struct Version {
    pub(crate) files: Vec<Vec<FileMetadata>>,
}

impl Version {
    pub(crate) fn new(max_levels: usize) -> Self {
        Self {
            files: vec![Vec::new(); max_levels],
        }
    }

    pub(crate) fn num_files_at_level(&self, level: usize) -> usize {
        self.files.get(level).map(|f| f.len()).unwrap_or(0)
    }

    pub(crate) fn get_files_at_level(&self, level: usize) -> &[FileMetadata] {
        self.files.get(level).map(|f| f.as_slice()).unwrap_or(&[])
    }

    pub(crate) fn level_size(&self, level: usize) -> u64 {
        self.files
            .get(level)
            .map(|files| files.iter().map(|f| f.file_size).sum())
            .unwrap_or(0)
    }

    #[allow(dead_code)]
    pub(crate) fn all_file_numbers(&self) -> Vec<u64> {
        self.files
            .iter()
            .flat_map(|level| level.iter().map(|f| f.file_number))
            .collect()
    }
}

pub(crate) struct VersionSet {
    #[allow(dead_code)]
    db_path: std::path::PathBuf,
    current: Version,
    next_file_number: AtomicU64,
    last_sequence: AtomicU64,
    max_levels: usize,
    #[allow(dead_code)]
    manifest_file_number: u64,
    #[allow(dead_code)]
    manifest_writer: Option<ManifestWriter>,
}

impl VersionSet {
    pub(crate) fn new(max_levels: usize) -> Self {
        Self {
            db_path: std::path::PathBuf::new(),
            current: Version::new(max_levels),
            next_file_number: AtomicU64::new(1),
            last_sequence: AtomicU64::new(0),
            max_levels,
            manifest_file_number: 1,
            manifest_writer: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn recover(db_path: &Path, max_levels: usize) -> Result<Self> {
        let mut vs = Self {
            db_path: db_path.to_path_buf(),
            current: Version::new(max_levels),
            next_file_number: AtomicU64::new(1),
            last_sequence: AtomicU64::new(0),
            max_levels,
            manifest_file_number: 1,
            manifest_writer: None,
        };

        if let Some(manifest_name) = read_current(db_path).await? {
            let manifest_path = db_path.join(&manifest_name);
            if manifest_path.exists() {
                let mut reader = ManifestReader::open(&manifest_path).await?;
                let edits = reader.read_all().await?;

                for edit in edits {
                    vs.apply_internal(&edit);
                }

                if let Some(num_str) = manifest_name.strip_prefix("MANIFEST-") {
                    if let Ok(num) = num_str.parse::<u64>() {
                        vs.manifest_file_number = num;
                    }
                }

                let writer = ManifestWriter::open_append(&manifest_path).await?;
                vs.manifest_writer = Some(writer);
            }
        }

        if vs.manifest_writer.is_none() {
            vs.create_new_manifest().await?;
        }

        Ok(vs)
    }

    #[allow(dead_code)]
    async fn create_new_manifest(&mut self) -> Result<()> {
        self.manifest_file_number = self.next_file_number.fetch_add(1, Ordering::SeqCst);
        let manifest_name = format!("MANIFEST-{:06}", self.manifest_file_number);
        let manifest_path = self.db_path.join(&manifest_name);

        let mut writer = ManifestWriter::create(&manifest_path).await?;

        let mut snapshot_edit = VersionEdit::new();
        snapshot_edit.set_next_file_number(self.next_file_number.load(Ordering::SeqCst));
        snapshot_edit.set_last_sequence(self.last_sequence.load(Ordering::SeqCst));

        for (level, files) in self.current.files.iter().enumerate() {
            for file in files {
                snapshot_edit.add_file(
                    level,
                    file.file_number,
                    file.file_size,
                    file.min_key.clone(),
                    file.max_key.clone(),
                );
            }
        }

        writer.write_edit(&snapshot_edit).await?;
        writer.sync().await?;

        write_current(&self.db_path, &manifest_name).await?;

        self.manifest_writer = Some(writer);
        Ok(())
    }

    pub(crate) fn current(&self) -> &Version {
        &self.current
    }

    pub(crate) fn next_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub(crate) fn peek_file_number(&self) -> u64 {
        self.next_file_number.load(Ordering::SeqCst)
    }

    pub(crate) fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    pub(crate) fn set_last_sequence(&self, seq: u64) {
        self.last_sequence.store(seq, Ordering::SeqCst);
    }

    pub(crate) fn increment_sequence(&self) -> u64 {
        self.last_sequence.fetch_add(1, Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub(crate) async fn log_and_apply(&mut self, edit: &VersionEdit) -> Result<()> {
        if let Some(ref mut writer) = self.manifest_writer {
            writer.write_edit(edit).await?;
            writer.sync().await?;
        }

        self.apply_internal(edit);
        Ok(())
    }

    pub(crate) fn apply(&mut self, edit: &VersionEdit) {
        self.apply_internal(edit);
    }

    fn apply_internal(&mut self, edit: &VersionEdit) {
        if let Some(num) = edit.next_file_number {
            self.next_file_number.store(num, Ordering::SeqCst);
        }
        if let Some(seq) = edit.last_sequence {
            self.last_sequence.store(seq, Ordering::SeqCst);
        }

        let mut deleted: HashMap<(usize, u64), bool> = HashMap::new();
        for del in &edit.deleted_files {
            deleted.insert((del.level, del.file_number), true);
        }

        let mut new_version = Version::new(self.max_levels);
        for (level, files) in self.current.files.iter().enumerate() {
            for file in files {
                if !deleted.contains_key(&(level, file.file_number)) {
                    new_version.files[level].push(file.clone());
                }
            }
        }

        for new_file in &edit.new_files {
            new_version.files[new_file.level].push(FileMetadata {
                file_number: new_file.file_number,
                file_size: new_file.file_size,
                min_key: new_file.min_key.clone(),
                max_key: new_file.max_key.clone(),
            });
        }

        for level in 1..self.max_levels {
            new_version.files[level].sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }

        self.current = new_version;
    }

    #[allow(dead_code)]
    pub(crate) fn manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }

    #[allow(dead_code)]
    pub(crate) fn num_levels(&self) -> usize {
        self.max_levels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_new() {
        let version = Version::new(7);
        assert_eq!(version.files.len(), 7);
        for level in 0..7 {
            assert_eq!(version.num_files_at_level(level), 0);
        }
    }

    #[test]
    fn test_version_set_apply() {
        let mut vs = VersionSet::new(7);

        let mut edit = VersionEdit::new();
        edit.set_next_file_number(10);
        edit.set_last_sequence(100);
        edit.add_file(0, 1, 1000, Bytes::from("aaa"), Bytes::from("zzz"));

        vs.apply(&edit);

        assert_eq!(vs.current().num_files_at_level(0), 1);
        assert_eq!(vs.last_sequence(), 100);
    }

    #[test]
    fn test_version_set_delete_file() {
        let mut vs = VersionSet::new(7);

        let mut edit1 = VersionEdit::new();
        edit1.add_file(0, 1, 1000, Bytes::from("aaa"), Bytes::from("zzz"));
        vs.apply(&edit1);
        assert_eq!(vs.current().num_files_at_level(0), 1);

        let mut edit2 = VersionEdit::new();
        edit2.delete_file(0, 1);
        vs.apply(&edit2);
        assert_eq!(vs.current().num_files_at_level(0), 0);
    }

    #[test]
    fn test_version_set_next_file_number() {
        let vs = VersionSet::new(7);
        assert_eq!(vs.next_file_number(), 1);
        assert_eq!(vs.next_file_number(), 2);
        assert_eq!(vs.next_file_number(), 3);
    }

    #[test]
    fn test_version_set_sequence() {
        let vs = VersionSet::new(7);
        assert_eq!(vs.last_sequence(), 0);
        vs.set_last_sequence(100);
        assert_eq!(vs.last_sequence(), 100);
        assert_eq!(vs.increment_sequence(), 100);
        assert_eq!(vs.last_sequence(), 101);
    }

    #[test]
    fn test_version_level_size() {
        let mut vs = VersionSet::new(7);

        let mut edit = VersionEdit::new();
        edit.add_file(0, 1, 1000, Bytes::from("a"), Bytes::from("b"));
        edit.add_file(0, 2, 2000, Bytes::from("c"), Bytes::from("d"));
        vs.apply(&edit);

        assert_eq!(vs.current().level_size(0), 3000);
    }

    #[test]
    fn test_version_get_files_at_level() {
        let mut vs = VersionSet::new(7);

        let mut edit = VersionEdit::new();
        edit.add_file(0, 1, 1000, Bytes::from("a"), Bytes::from("b"));
        vs.apply(&edit);

        let files = vs.current().get_files_at_level(0);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_number, 1);
    }

    #[test]
    fn test_version_set_manifest_file_number() {
        let vs = VersionSet::new(7);
        assert_eq!(vs.manifest_file_number(), 1);
    }

    #[test]
    fn test_version_set_num_levels() {
        let vs = VersionSet::new(7);
        assert_eq!(vs.num_levels(), 7);
    }
}
