use crate::manifest::{FileMetadata, Version};

pub(crate) struct CompactionTask {
    pub(crate) level: usize,
    pub(crate) input_files: Vec<FileMetadata>,
    pub(crate) output_level: usize,
}

pub(crate) struct CompactionPicker {
    l0_compaction_trigger: usize,
    level_size_multiplier: usize,
    base_level_size: u64,
    max_levels: usize,
}

impl CompactionPicker {
    pub(crate) fn new(
        l0_compaction_trigger: usize,
        level_size_multiplier: usize,
        base_level_size: u64,
        max_levels: usize,
    ) -> Self {
        Self {
            l0_compaction_trigger,
            level_size_multiplier,
            base_level_size,
            max_levels,
        }
    }

    pub(crate) fn pick_compaction(&self, version: &Version) -> Option<CompactionTask> {
        if version.num_files_at_level(0) >= self.l0_compaction_trigger {
            return Some(self.pick_l0_compaction(version));
        }

        for level in 1..self.max_levels - 1 {
            let level_size = version.level_size(level);
            let target_size = self.target_size_for_level(level);
            if level_size > target_size {
                return Some(self.pick_level_compaction(version, level));
            }
        }

        None
    }

    fn pick_l0_compaction(&self, version: &Version) -> CompactionTask {
        let input_files: Vec<FileMetadata> = version.get_files_at_level(0).to_vec();

        CompactionTask {
            level: 0,
            input_files,
            output_level: 1,
        }
    }

    fn pick_level_compaction(&self, version: &Version, level: usize) -> CompactionTask {
        let files = version.get_files_at_level(level);
        let input_file = files.first().cloned().unwrap();

        CompactionTask {
            level,
            input_files: vec![input_file],
            output_level: level + 1,
        }
    }

    fn target_size_for_level(&self, level: usize) -> u64 {
        let mut size = self.base_level_size;
        for _ in 1..level {
            size *= self.level_size_multiplier as u64;
        }
        size
    }

    #[allow(dead_code)]
    pub(crate) fn needs_compaction(&self, version: &Version) -> bool {
        self.pick_compaction(version).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::VersionEdit;
    use crate::manifest::VersionSet;
    use bytes::Bytes;

    #[test]
    fn test_picker_l0_compaction() {
        let mut vs = VersionSet::new(7);

        for i in 0..4 {
            let mut edit = VersionEdit::new();
            edit.add_file(0, i, 1000, Bytes::from("a"), Bytes::from("z"));
            vs.apply(&edit);
        }

        let picker = CompactionPicker::new(4, 10, 10 * 1024 * 1024, 7);
        let task = picker.pick_compaction(vs.current()).unwrap();

        assert_eq!(task.level, 0);
        assert_eq!(task.output_level, 1);
        assert_eq!(task.input_files.len(), 4);
    }

    #[test]
    fn test_picker_no_compaction_needed() {
        let vs = VersionSet::new(7);

        let picker = CompactionPicker::new(4, 10, 10 * 1024 * 1024, 7);
        assert!(picker.pick_compaction(vs.current()).is_none());
    }

    #[test]
    fn test_picker_level_compaction() {
        let mut vs = VersionSet::new(7);

        let mut edit = VersionEdit::new();
        edit.add_file(1, 1, 20 * 1024 * 1024, Bytes::from("a"), Bytes::from("z"));
        vs.apply(&edit);

        let picker = CompactionPicker::new(4, 10, 10 * 1024 * 1024, 7);
        let task = picker.pick_compaction(vs.current()).unwrap();

        assert_eq!(task.level, 1);
        assert_eq!(task.output_level, 2);
    }

    #[test]
    fn test_picker_needs_compaction() {
        let mut vs = VersionSet::new(7);

        let picker = CompactionPicker::new(4, 10, 10 * 1024 * 1024, 7);
        assert!(!picker.needs_compaction(vs.current()));

        for i in 0..4 {
            let mut edit = VersionEdit::new();
            edit.add_file(0, i, 1000, Bytes::from("a"), Bytes::from("z"));
            vs.apply(&edit);
        }

        assert!(picker.needs_compaction(vs.current()));
    }

    #[test]
    fn test_target_size_for_level() {
        let picker = CompactionPicker::new(4, 10, 10 * 1024 * 1024, 7);
        assert_eq!(picker.target_size_for_level(1), 10 * 1024 * 1024);
        assert_eq!(picker.target_size_for_level(2), 100 * 1024 * 1024);
        assert_eq!(picker.target_size_for_level(3), 1000 * 1024 * 1024);
    }
}
