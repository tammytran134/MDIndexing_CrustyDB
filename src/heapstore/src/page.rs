use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use std::cmp::max;
use std::collections::HashMap;
use std::convert::TryInto;
use std::mem::{size_of, size_of_val};

pub type ValAddr = u16;


#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// A hash slot is the value for the header's slot_arr hashmap, with slot_id being the key
/// It holds information about a slot's data:
/// start field represents the start offset of where the data is held in
/// the page's data array, and end field represents the end offset of where
/// the daa is held in the page's data array
pub struct HashSlot {
    pub start: ValAddr,
    pub end: ValAddr,
}

/// A vec slot is used mainly for sorting, and holds information about
/// a slot: its slot_id, its start field represents the start offset of where the data is held in
/// the page's data array, and its end field represents the end offset of where
/// the daa is held in the page's data array
#[derive(Copy, Clone)]
pub struct VecSlot {
    start: ValAddr,
    slot_id: SlotId,
    end: ValAddr,
}

/// Struct for the Header of the page, which contains page_id of the page,
/// number of slots/records the page currently has,
/// and a hashmap of a value's slot id and its corresponding hash slot
pub struct Header {
    pub page_id: PageId,
    pub num_slot: u16,
    pub slot_arr: HashMap<SlotId, HashSlot>,
}

/// The struct for a page. Note this can hold more elements/meta data when created,
/// but it must be able to be packed/serialized/marshalled into the data array of size
/// PAGE_SIZE. In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// You do not need reclaim header information for a value inserted (eg 6 bytes per value ever inserted)
/// The rest must filled as much as possible to hold values.
pub(crate) struct Page {
    /// The data for data
    pub header: Header,
    pub data: [u8; PAGE_SIZE],
}

/// The functions required for page
impl Page {
    /// Create a new page
    pub fn new(page_id: PageId) -> Self {
        let new_header = Header {
            page_id: page_id,
            num_slot: 0,
            slot_arr: HashMap::new(),
        };
        Page {
            header: new_header,
            data: [0; PAGE_SIZE],
        }
    }

    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId {
        self.header.page_id
    }

    /// Get the size of the data in bytes from its start and end offset
    pub fn get_slot_size(&self, slot: &VecSlot) -> usize {
        usize::from(slot.end - slot.start)
    }

    /// Convert the slot_arr hashmap in the header to an array of VecSlot
    pub fn hash_to_vec_slot(&self) -> Vec<VecSlot> {
        let mut vec_slot = Vec::new();
        for (slot_id, hash_slot) in &self.header.slot_arr {
            vec_slot.push(VecSlot {
                start: hash_slot.start,
                slot_id: *slot_id,
                end: hash_slot.end,
            });
        }
        vec_slot
    }

    /// Shift all data to end of page to eliminate fragmentation
    /// In the end, after data consolidation,
    /// return the beginning offset that can be used
    /// to write new data, given its data_size
    pub fn fix_fragmentation(&mut self, data_size: usize) -> Option<ValAddr> {
        let mut slot_arr = self.hash_to_vec_slot();
        slot_arr.sort_unstable_by(|a, b| b.start.cmp(&a.start));
        let mut curr_end = PAGE_SIZE;
        for slot in slot_arr {
            let slot_size = self.get_slot_size(&slot);
            let mut slot_data = vec![0; slot_size];
            slot_data[0..slot_size]
                .clone_from_slice(&self.data[usize::from(slot.start)..usize::from(slot.end)]);
            self.data[(curr_end - slot_size)..curr_end].clone_from_slice(&slot_data);
            curr_end -= slot_size;
        }
        Some((curr_end - data_size).try_into().unwrap())
    }

    /// Get the first free space in the page that can hold data of size data_size
    /// as input argument
    /// The function returns Some(x), which represents the first free space
    /// counting from the end of the array, and None if the page doesn't have
    /// enough space
    pub fn get_first_free_space(&mut self, data_size: usize) -> Option<ValAddr> {
        let num_slot = self.header.slot_arr.len();
        if num_slot == 0 {
            return Some(u16::try_from(PAGE_SIZE - data_size).unwrap());
        }
        let mut total_size: usize = 0;
        let mut res = None;
        let mut slot_arr = self.hash_to_vec_slot();
        slot_arr.sort_unstable_by(|a, b| a.start.cmp(&b.start));
        // println!("What's wrong 1 {}", usize::from(slot_arr[0].start)
        // - self.get_header_size());
        let beginning_spot = slot_arr[0].start as i16
        - self.get_header_size() as i16
        - (size_of_val(&slot_arr[0].slot_id)
            + size_of_val(&slot_arr[0].start)
            + size_of_val(&slot_arr[0].end)) as i16;
        if beginning_spot >= data_size as i16
        {
            res = Some(slot_arr[0].start - u16::try_from(data_size).unwrap());
        }
        for i in 0..(num_slot - 1) {
            let slot_size = self.get_slot_size(&slot_arr[i]);
            total_size += slot_size;
            let diff = slot_arr[i + 1].start - slot_arr[i].end;
            if diff > 0 && usize::from(diff) >= data_size {
                res = Some(slot_arr[i].end);
            }
        }
        total_size += self.get_slot_size(&slot_arr[num_slot - 1]);
        if (PAGE_SIZE - self.get_header_size() - total_size)
            < (data_size
                + size_of_val(&slot_arr[0].slot_id)
                + size_of_val(&slot_arr[0].start)
                + size_of_val(&slot_arr[0].end))
        {
            None
        } else {
            match res {
                None => self.fix_fragmentation(data_size), // DATA SHIFT
                Some(x) => Some(x),
            }
        }
    }

    /// Generate new slot_id for new inserted data
    pub fn generate_slot_id(&self) -> SlotId {
        let mut slot_arr = self.hash_to_vec_slot();
        slot_arr.sort_unstable_by(|a, b| a.slot_id.cmp(&b.slot_id));
        let mut slot_id = 0;
        for single_slot in slot_arr {
            if single_slot.slot_id <= slot_id {
                slot_id += 1;
            } else {
                return slot_id;
            }
        }
        slot_id
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should replace the slotId on the next insert.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        let data_size = bytes.len();
        let first_free_space = self.get_first_free_space(data_size);
        let mut start_offset: ValAddr = 0;
        match first_free_space {
            None => return None,
            Some(val_addr) => start_offset = val_addr,
        }
        let new_slot_id = self.generate_slot_id();
        let end_offset = start_offset + u16::try_from(data_size).unwrap();
        self.header.slot_arr.insert(
            new_slot_id,
            HashSlot {
                start: start_offset,
                end: end_offset,
            },
        );
        self.data[start_offset.try_into().unwrap()..end_offset.try_into().unwrap()]
            .clone_from_slice(bytes);
        self.header.num_slot += 1;
        Some(new_slot_id)
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        match &self.header.slot_arr.get(&slot_id) {
            Some(hash_slot) =>
                {return Some(
                    self.data
                        [hash_slot.start.try_into().unwrap()..hash_slot.end.try_into().unwrap()]
                        .to_vec(),
                )},
            None => None,
        }
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        match &self.header.slot_arr.remove(&slot_id) {
            Some(_) => {
                self.header.num_slot -= 1;
                Some(())
            }
            None => None,
        }
    }

    /// Create a new page from the byte array.
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut curr_start = 0;
        let page_id = u16::from_be_bytes(data[curr_start..(curr_start + 2)].try_into().unwrap());
        let mut page = Page::new(page_id);
        curr_start += 2;
        let mut num_slot =
            u16::from_be_bytes(data[curr_start..(curr_start + 2)].try_into().unwrap());
        curr_start += 2;
        page.header.num_slot = num_slot;
        while num_slot > 0 {
            let slot_id =
                u16::from_be_bytes(data[curr_start..(curr_start + 2)].try_into().unwrap());
            curr_start += 2;
            let slot_start =
                u16::from_be_bytes(data[curr_start..(curr_start + 2)].try_into().unwrap());
            curr_start += 2;
            let slot_end =
                u16::from_be_bytes(data[curr_start..(curr_start + 2)].try_into().unwrap());
            curr_start += 2;
            let slot_data = &data[curr_start..(curr_start + usize::from(slot_end - slot_start))];
            page.header.slot_arr.insert(
                slot_id,
                HashSlot {
                    start: slot_start,
                    end: slot_end,
                },
            );
            page.data[slot_start.try_into().unwrap()..slot_end.try_into().unwrap()]
                .clone_from_slice(slot_data);
            curr_start += usize::from(slot_end - slot_start);
            num_slot -= 1;
        }
        page
    }

    /// Convert a page into bytes. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    pub fn get_bytes(&self) -> Vec<u8> {
        let mut res = vec![0; PAGE_SIZE];
        let mut curr_start = 0;
        res[curr_start..(curr_start + 2)].clone_from_slice(&self.header.page_id.to_be_bytes());
        curr_start += 2;
        res[curr_start..(curr_start + 2)].clone_from_slice(&self.header.num_slot.to_be_bytes());
        curr_start += 2;
        let mut slot_arr = self.hash_to_vec_slot();
        slot_arr.sort_unstable_by(|a, b| b.start.cmp(&a.start));
        for slot in slot_arr {
            res[curr_start..(curr_start + 2)].clone_from_slice(&slot.slot_id.to_be_bytes());
            curr_start += 2;
            res[curr_start..(curr_start + 2)].clone_from_slice(&slot.start.to_be_bytes());
            curr_start += 2;
            res[curr_start..(curr_start + 2)].clone_from_slice(&slot.end.to_be_bytes());
            curr_start += 2;
            res[curr_start..(curr_start + usize::from(slot.end - slot.start))]
                .clone_from_slice(
                    &self.data[usize::from(slot.start)..usize::from(slot.end)],
                );
            curr_start += usize::from(slot.end - slot.start);
        }
        res
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        let mut header_size =
            size_of_val(&self.header.page_id) + size_of_val(&self.header.num_slot);
        for (slot_id, hash_slot) in &self.header.slot_arr {
            header_size += size_of_val(&*slot_id);
            header_size += size_of_val(&hash_slot.start);
            header_size += size_of_val(&hash_slot.end);
        }
        header_size
    }

    /// A utility function to determine the largest block of free space in the page.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {
        let num_slot = self.header.slot_arr.len();
        if num_slot == 0 {
            return PAGE_SIZE - self.get_header_size();
        }
        let mut slot_arr = self.hash_to_vec_slot();
        slot_arr.sort_unstable_by(|a, b| a.start.cmp(&b.start));
        let mut res =
            PAGE_SIZE - self.get_header_size() - (PAGE_SIZE - usize::from(slot_arr[0].start));
        for i in 0..(num_slot - 1) {
            res = max(res, (slot_arr[i + 1].start - slot_arr[i].end).into());
        }
        res
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
/// See https://stackoverflow.com/questions/30218886/how-to-implement-iterator-and-intoiterator-for-a-simple-struct
pub struct PageIter {
    slot_vec: Vec<VecSlot>,
    data: [u8; PAGE_SIZE],
    index: usize,
}

impl PageIter {
    pub fn gen_empty_pg_iter() -> Self {
        PageIter{slot_vec: Vec::new(), data: [0; PAGE_SIZE], index: 0}
    }
}

/// The implementation of the (consuming) page iterator.
impl Iterator for PageIter {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut res: Vec<u8> = Vec::new();
        let slot = &self.slot_vec.get(self.index);
        self.index += 1;
        match slot {
            Some(vec_slot) => {
                res = vec![0; (vec_slot.end - vec_slot.start).try_into().unwrap()];
                res[0..usize::from(vec_slot.end - vec_slot.start)].clone_from_slice(
                    &self.data[usize::from(vec_slot.start)..usize::from(vec_slot.end)],
                );
                Some(res)
            }
            None => None,
        }
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = Vec<u8>;
    type IntoIter = PageIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut slot_arr = self.hash_to_vec_slot();
        slot_arr.sort_unstable_by(|a, b| a.slot_id.cmp(&b.slot_id));
        PageIter {
            slot_vec: slot_arr,
            data: self.data,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(
            PAGE_SIZE - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        //println!("Data size is {}", byte_len);
        //println!("Header size is {}", p.get_header_size());
        //println!("Slot 0 start is {}", &p.header.slot_arr[0].start);
        //println!("{:?}", &p.header.slot_arr);
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_largest_free_contiguous_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let mut p = Page::new(0);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.get_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.get_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.get_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.get_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes3.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.get_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(None, iter.next());
    }
}