use crate::Result;

use super::decompress;
use super::skiplist::SkipList;
use crate::common::BinarySerializable;
use crate::common::HasLen;
use crate::common::VInt;
use crate::directory::ReadOnlySource;
use crate::schema::Document;
use crate::space_usage::StoreSpaceUsage;
use crate::DocId;
use std::cell::RefCell;
use std::io;
use std::io::Read;
use std::mem::size_of;

/// Reads document off tantivy's [`Store`](./index.html)
#[derive(Clone)]
pub struct StoreReader {
    data: ReadOnlySource,
    offset_index_source: ReadOnlySource,
    current_block_offset: RefCell<usize>,
    current_block: RefCell<Vec<u8>>,
    max_doc: DocId,
}

impl StoreReader {
    /// Opens a store reader
    pub fn from_source(data: ReadOnlySource) -> StoreReader {
        let (data_source, offset_index_source, max_doc) = split_source(data);
        StoreReader {
            data: data_source,
            offset_index_source,
            current_block_offset: RefCell::new(usize::max_value()),
            current_block: RefCell::new(Vec::new()),
            max_doc,
        }
    }

    pub(crate) fn block_index(&self) -> SkipList<u64> {
        SkipList::from(self.offset_index_source.clone())
    }

    fn block_offset(&self, doc_id: DocId) -> (DocId, u64) {
        self.block_index()
            .seek(u64::from(doc_id) + 1)
            .map(|(doc, offset)| (doc as DocId, offset))
            .unwrap_or((0u32, 0u64))
    }

    pub(crate) fn block_data(&mut self) -> Vec<u8> {
        self.data.read_all().expect("Can't read block data")
    }

    fn compressed_block(&self, addr: usize) -> Vec<u8> {
        let mut buffer_slice = self.data.slice_from(addr);
        let block_len = u32::deserialize(&mut buffer_slice).expect("") as usize;
        let mut block = vec![0u8; block_len];
        buffer_slice
            .read_exact(&mut block)
            .expect("Can't read compressed block");
        block
    }

    fn read_block(&self, block_offset: usize) -> io::Result<()> {
        if block_offset != *self.current_block_offset.borrow() {
            let mut current_block_mut = self.current_block.borrow_mut();
            current_block_mut.clear();
            let compressed_block = self.compressed_block(block_offset);
            decompress(&compressed_block, &mut current_block_mut)?;
            *self.current_block_offset.borrow_mut() = block_offset;
        }
        Ok(())
    }

    /// Reads a given document.
    ///
    /// Calling `.get(doc)` is relatively costly as it requires
    /// decompressing a LZ4-compressed block.
    ///
    /// It should not be called to score documents
    /// for instance.
    pub fn get(&self, doc_id: DocId) -> Result<Document> {
        let (first_doc_id, block_offset) = self.block_offset(doc_id);
        self.read_block(block_offset as usize)?;
        let current_block_mut = self.current_block.borrow_mut();
        let mut cursor = &current_block_mut[..];
        for _ in first_doc_id..doc_id {
            let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
            cursor = &cursor[doc_length..];
        }
        let doc_length = VInt::deserialize(&mut cursor)?.val() as usize;
        cursor = &cursor[..doc_length];
        Ok(Document::deserialize(&mut cursor)?)
    }

    /// Summarize total space usage of this store reader.
    pub fn space_usage(&self) -> StoreSpaceUsage {
        StoreSpaceUsage::new(self.data.len(), self.offset_index_source.len())
    }
}

fn split_source(data: ReadOnlySource) -> (ReadOnlySource, ReadOnlySource, DocId) {
    let data_len = data.len();
    let footer_offset = data_len - size_of::<u64>() - size_of::<u32>();
    let mut serialized_offset: ReadOnlySource = data.slice(footer_offset, data_len);
    let offset = u64::deserialize(&mut serialized_offset).unwrap();
    let offset = offset as usize;
    let max_doc = u32::deserialize(&mut serialized_offset).unwrap();
    (
        data.slice(0, offset),
        data.slice(offset, footer_offset),
        max_doc,
    )
}
