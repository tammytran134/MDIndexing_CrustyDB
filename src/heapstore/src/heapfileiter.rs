use crate::heapfile::HeapFile;
use crate::page::PageIter;
use common::ids::{ContainerId, PageId, TransactionId};
use std::sync::{Arc, RwLock};
use std::mem;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    hf: Arc<HeapFile>,
    index: Arc<RwLock<PageId>>,
    curr_pg_iter: Arc<RwLock<PageIter>>,
    end_of_page: Arc<RwLock<bool>>,

}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(_container_id: ContainerId, _tid: TransactionId, hf: Arc<HeapFile>) -> Self {    
        HeapFileIterator {hf: hf, index: Arc::new(RwLock::new(0)), 
            curr_pg_iter: Arc::new(RwLock::new(PageIter::gen_empty_pg_iter())), end_of_page: Arc::new(RwLock::new(true))}
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        let curr_num_pg = *self.hf.num_page.read().unwrap();
        if curr_num_pg == 0 || curr_num_pg <= *self.index.read().unwrap() {
            return None;
        }
        match *self.end_of_page.read().unwrap() {
            //None => {self.curr_pg_iter = Arc::new(Some(self.hf.read_page_from_file(*self.index).unwrap().into_iter()))},
            true => {*self.curr_pg_iter.write().unwrap() = self.hf.read_page_from_file(*self.index.read().unwrap()).unwrap().into_iter(); *self.end_of_page.write().unwrap() = false},
            false => (),
        }
        {
            match &self.curr_pg_iter.clone().write().unwrap().next() {
                Some(data) => Some(data.clone()),
                None => {*self.index.write().unwrap() += 1; *self.curr_pg_iter.write().unwrap() = PageIter::gen_empty_pg_iter(); 
                    *self.end_of_page.write().unwrap() = true; return self.next();}
        }
    }
    }
}
