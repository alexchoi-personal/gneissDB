mod reader;
mod writer;

pub(crate) use reader::WalReader;
pub(crate) use writer::WalWriter;

use crate::types::SequenceNumber;
use bytes::Bytes;

#[derive(Clone, Debug)]
pub(crate) enum WalRecord {
    Put {
        sequence: SequenceNumber,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        sequence: SequenceNumber,
        key: Bytes,
    },
    Batch {
        sequence: SequenceNumber,
        ops: Vec<BatchOp>,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum BatchOp {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

pub(crate) const RECORD_TYPE_PUT: u8 = 0x01;
pub(crate) const RECORD_TYPE_DELETE: u8 = 0x02;
pub(crate) const RECORD_TYPE_BATCH: u8 = 0x03;

pub(crate) const BATCH_OP_PUT: u8 = 0x01;
pub(crate) const BATCH_OP_DELETE: u8 = 0x02;
