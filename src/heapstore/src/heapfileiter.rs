use crate::heapfile::HeapFile;
use crate::page::PageIter;
use common::ids::{ContainerId, PageId, TransactionId};
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    hf: Arc<HeapFile>,
    index: PageId,
    // Iterator of most currently accessed page
    curr_pg_iter: PageIter,
    // if the iterator is at the end of most currently accessed page or not
    end_of_page: bool,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(_container_id: ContainerId, _tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        HeapFileIterator {
            hf,
            index: 0,
            curr_pg_iter: PageIter::gen_empty_pg_iter(),
            end_of_page: true,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
/// Recursive function to keep calling next until we are not at the end of a page
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        let curr_num_pg = *self.hf.num_page.read().unwrap();
        if curr_num_pg == 0 || curr_num_pg <= self.index {
            return None;
        }
        match self.end_of_page {
            //None => {self.curr_pg_iter = Arc::new(Some(self.hf.read_page_from_file(*self.index).unwrap().into_iter()))},
            true => { // if at end of current page, set curr_pg_iter to the iterator of the next page
                self.curr_pg_iter = self.hf.read_page_from_file(self.index).unwrap().into_iter();
                self.end_of_page = false
            }
            false => (),
        }
        {
            match &self.curr_pg_iter.next() {
                Some(data) => Some(data.clone()),
                None => { // If at end of page, increment index to access next page and call next recursively
                    self.index += 1; 
                    self.end_of_page = true;
                    self.next()
                }
            }
        }
    }
}
