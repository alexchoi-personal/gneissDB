use bytes::Bytes;

#[derive(Clone, Debug, Default)]
pub(crate) struct VersionEdit {
    pub(crate) next_file_number: Option<u64>,
    pub(crate) last_sequence: Option<u64>,
    pub(crate) new_files: Vec<NewFile>,
    pub(crate) deleted_files: Vec<DeletedFile>,
}

#[derive(Clone, Debug)]
pub(crate) struct NewFile {
    pub(crate) level: usize,
    pub(crate) file_number: u64,
    pub(crate) file_size: u64,
    pub(crate) min_key: Bytes,
    pub(crate) max_key: Bytes,
}

#[derive(Clone, Debug)]
pub(crate) struct DeletedFile {
    pub(crate) level: usize,
    pub(crate) file_number: u64,
}

impl VersionEdit {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    pub(crate) fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }

    #[allow(dead_code)]
    pub(crate) fn set_last_sequence(&mut self, seq: u64) {
        self.last_sequence = Some(seq);
    }

    pub(crate) fn add_file(
        &mut self,
        level: usize,
        file_number: u64,
        file_size: u64,
        min_key: Bytes,
        max_key: Bytes,
    ) {
        self.new_files.push(NewFile {
            level,
            file_number,
            file_size,
            min_key,
            max_key,
        });
    }

    pub(crate) fn delete_file(&mut self, level: usize, file_number: u64) {
        self.deleted_files.push(DeletedFile { level, file_number });
    }

    #[allow(dead_code)]
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = Vec::new();

        if let Some(num) = self.next_file_number {
            buf.push(0x01);
            buf.extend_from_slice(&num.to_le_bytes());
        }

        if let Some(seq) = self.last_sequence {
            buf.push(0x02);
            buf.extend_from_slice(&seq.to_le_bytes());
        }

        for file in &self.new_files {
            buf.push(0x03);
            buf.push(file.level as u8);
            buf.extend_from_slice(&file.file_number.to_le_bytes());
            buf.extend_from_slice(&file.file_size.to_le_bytes());
            buf.extend_from_slice(&(file.min_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&file.min_key);
            buf.extend_from_slice(&(file.max_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&file.max_key);
        }

        for file in &self.deleted_files {
            buf.push(0x04);
            buf.push(file.level as u8);
            buf.extend_from_slice(&file.file_number.to_le_bytes());
        }

        Bytes::from(buf)
    }

    #[allow(dead_code)]
    pub(crate) fn decode(data: &[u8]) -> Option<Self> {
        let mut edit = VersionEdit::new();
        let mut pos = 0;

        while pos < data.len() {
            let tag = data[pos];
            pos += 1;

            match tag {
                0x01 => {
                    if pos + 8 > data.len() {
                        return None;
                    }
                    edit.next_file_number =
                        Some(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
                    pos += 8;
                }
                0x02 => {
                    if pos + 8 > data.len() {
                        return None;
                    }
                    edit.last_sequence =
                        Some(u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?));
                    pos += 8;
                }
                0x03 => {
                    if pos + 17 > data.len() {
                        return None;
                    }
                    let level = data[pos] as usize;
                    pos += 1;
                    let file_number = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let file_size = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;
                    let min_key_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    let min_key = Bytes::copy_from_slice(&data[pos..pos + min_key_len]);
                    pos += min_key_len;
                    let max_key_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    let max_key = Bytes::copy_from_slice(&data[pos..pos + max_key_len]);
                    pos += max_key_len;

                    edit.new_files.push(NewFile {
                        level,
                        file_number,
                        file_size,
                        min_key,
                        max_key,
                    });
                }
                0x04 => {
                    if pos + 9 > data.len() {
                        return None;
                    }
                    let level = data[pos] as usize;
                    pos += 1;
                    let file_number = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
                    pos += 8;

                    edit.deleted_files.push(DeletedFile { level, file_number });
                }
                _ => return None,
            }
        }

        Some(edit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_edit_encode_decode() {
        let mut edit = VersionEdit::new();
        edit.set_next_file_number(100);
        edit.set_last_sequence(500);
        edit.add_file(0, 1, 1000, Bytes::from("aaa"), Bytes::from("zzz"));
        edit.delete_file(1, 2);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.next_file_number, Some(100));
        assert_eq!(decoded.last_sequence, Some(500));
        assert_eq!(decoded.new_files.len(), 1);
        assert_eq!(decoded.new_files[0].level, 0);
        assert_eq!(decoded.new_files[0].file_number, 1);
        assert_eq!(decoded.deleted_files.len(), 1);
        assert_eq!(decoded.deleted_files[0].level, 1);
    }

    #[test]
    fn test_version_edit_empty() {
        let edit = VersionEdit::new();
        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert!(decoded.next_file_number.is_none());
        assert!(decoded.last_sequence.is_none());
        assert!(decoded.new_files.is_empty());
        assert!(decoded.deleted_files.is_empty());
    }

    #[test]
    fn test_version_edit_decode_invalid() {
        let data = vec![0xff];
        assert!(VersionEdit::decode(&data).is_none());
    }
}
