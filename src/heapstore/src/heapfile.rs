use crate::page::Page;
use common::ids::PageId;
use common::{CrustyError, PAGE_SIZE};
use std::fs::{metadata, File, OpenOptions};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};
//use std::io::prelude::*;
//use std::io::BufWriter;
//use std::io::{Seek, SeekFrom};
//use std::collections::HashMap;

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    pub num_page: Arc<RwLock<PageId>>,
    pub heap_file: Arc<RwLock<File>>,
    // pub page_map: Arc<RwLock<HashMap<PageId, Page>>>,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    // Given a path to a file, get the number of pages it holds
    pub(crate) fn get_num_page_from_file(file_path: &Path) -> PageId {
        u16::try_from(metadata(file_path).unwrap().len() as usize / PAGE_SIZE).unwrap()
    }

    // pub(crate) fn deserialize_page_from_file(num_page: PageId, file: &File) -> HashMap<PageId, Page> {
    //     let mut res = HashMap::new();
    //     for i in 0..num_page {
    //         let start_offset = usize::from(i) * PAGE_SIZE;
    //         let mut buf = [0u8; PAGE_SIZE];
    //         file.read_at(&mut buf, start_offset.try_into().unwrap());
    //         let page = Page::from_bytes(&buf);
    //         res.insert(i, page);
    //     }
    //     res
    // }

    /// Create a new heapfile for the given path and container Id. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };
        let num_page = HeapFile::get_num_page_from_file(&file_path);
        // let mut page_map = HashMap::new();
        // for i in 0..num_page {
        //     let start_offset = usize::from(i) * PAGE_SIZE;
        //     let mut buf = [0u8; PAGE_SIZE];
        //     file.read_at(&mut buf, start_offset.try_into().unwrap());
        //     let page = Page::from_bytes(&buf);
        //     page_map.insert(i, page);
        // }
        Ok(HeapFile {
            num_page: Arc::new(RwLock::new(num_page)),
            heap_file: Arc::new(RwLock::new(file)),
            //page_map: Arc::new(RwLock::new(page_map)),
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        *self.num_page.read().unwrap()
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        //check valid pids
        if pid >= self.num_pages() {
            return Err(CrustyError::CrustyError(String::from("pid invalid")));
        }
        //let page = *self.page_map.read().unwrap().get(&pid).unwrap();
        let start_offset = usize::from(pid) * PAGE_SIZE;
        let mut buf = [0u8; PAGE_SIZE];
        self.heap_file
            .read()
            .unwrap()
            .read_at(&mut buf, start_offset.try_into().unwrap())
            .expect("Can't read data from heap file");
        let page = Page::from_bytes(&buf);
        Ok(page)
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        let start_offset = usize::from(*self.num_page.read().unwrap()) * PAGE_SIZE;
        let buf = page.get_bytes();
        self.heap_file
            .write()
            .unwrap()
            .write_at(&buf, start_offset.try_into().unwrap())?;
        //self.page_map.write().unwrap().insert(*self.num_page.read().unwrap(), page);
        *self.num_page.write().unwrap() += 1;
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf()).unwrap();

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.get_bytes();
        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.get_bytes();

        hf.write_page_to_file(p1);
        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.get_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
