use crate::heapfile::{HeapFile, KdIndex, RIndex};
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::Field;
use common::testutil::gen_random_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use common::md_index::KdTree;
use common::md_index::R_Tree;

#[derive(Clone, Serialize, Deserialize)]
pub struct SerializedHeapFile {
    pub hf_path: Arc<RwLock<PathBuf>>,
}

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    // Mapping of Container to a serializable struct
    pub hf_serialized_map: Arc<RwLock<HashMap<ContainerId, SerializedHeapFile>>>,
    #[serde(skip)]
    // Mapping of Container to its corresponding HeapFile struct
    hf_map: Arc<RwLock<HashMap<ContainerId, HeapFile>>>,
    /// Path to database metadata files.
    pub storage_path: String,
    is_temp: bool,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        match &self.hf_map.read().unwrap().get(&container_id) {
            // if container exists
            None => None,
            Some(heap_file) => {
                match heap_file.read_page_from_file(page_id) {
                    // if read page succeeds
                    Err(_) => None,
                    Ok(page) => Some(page),
                }
            }
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        match &self.hf_map.read().unwrap().get(&container_id) {
            // if container exists
            None => Err(CrustyError::CrustyError(String::from(
                "Couldn't find Container",
            ))),
            Some(heap_file) => {
                match heap_file.write_page_to_file(page) {
                    // if write page succeeds
                    Err(_) => Err(CrustyError::CrustyError(String::from(
                        "Couldn't write page to file",
                    ))),
                    Ok(_) => Ok(()),
                }
            }
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        *self
            .hf_map
            .read()
            .unwrap()
            .get(&container_id)
            .unwrap()
            .num_page
            .read()
            .unwrap()
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        match &self.hf_map.read().unwrap().get(&container_id) {
            // if container exists
            None => (0, 0),
            Some(heap_file) => (
                heap_file.read_count.load(Ordering::Relaxed),
                heap_file.write_count.load(Ordering::Relaxed),
            ),
        }
    }

    /// Write modified page back to file
    pub(crate) fn write_updated_page_to_file(
        &self,
        container_id: ContainerId,
        page: &Page,
        page_id: PageId,
    ) -> Result<(), CrustyError> {
        let page_serialized = page.get_bytes();
        let hf_map = &self.hf_map.read().unwrap();
        let hf = hf_map.get(&container_id).unwrap();
        let underlying_file = hf.heap_file.write().unwrap();
        underlying_file.write_at(
            &page_serialized,
            (usize::from(page_id) * PAGE_SIZE).try_into().unwrap(),
        )?;
        Ok(())
    }

    fn get_attribute_list(attributes: &str) -> Vec<String> {
        let mut attributes_copy = &attributes.trim()[1..attributes.len()-1];
        let mut attribute_tokens = attributes_copy.split(",");
        let mut attribute_list = Vec::new();
        while let Some(single_attribute) = attribute_tokens.next() {
            attribute_list.push(single_attribute.trim().to_string());
        }
        if attribute_list.len() == 0 {
            //error
        }      
        attribute_list.clone()  
    }

    fn scan_tuple_for_range(tuple: &Tuple, min: &Vec<Field>, max: &Vec<Field>, idx_fields: &Vec<usize>) -> bool {
        if KdTree::if_within_range(&tuple.field_vals, min, max, idx_fields) {
            return true
        }
        else {
            return false
        }
    }

    pub fn create_index_by_id(&self, tree_type: &str, index_name: &str, container_id: ContainerId, attributes: &str, table: &Table) {
        debug!("Comes to create_index_by_id in Storage Manager");
        let hf_map = &self.hf_map.read().unwrap();
        let hf = hf_map.get(&container_id).unwrap();
        let hf_iterator = self.get_iterator(container_id, TransactionId::new(), Permissions::ReadOnly);
        let schema = &table.schema;
        let mut field_vec = Vec::new();
        let attribute_list = StorageManager::get_attribute_list(attributes);
        for attribute_name in attribute_list {
            let field_index = schema.get_field_index(&attribute_name);
            if field_index.is_some() {
                field_vec.push(*field_index.unwrap());
            }
            else {
                error!("Field not found");
            }
        }
        let mut bulk_load_data = Vec::new();
        for (i, val) in hf_iterator.enumerate() {
            let tuple = Tuple::from_bytes(&val);
            bulk_load_data.push(tuple.field_vals.clone());
        }
        debug!("Bulk load data array {:?}", &bulk_load_data);
        match tree_type {
            "KD" => { hf.kd_index_map.write().unwrap().insert(index_name.to_string(), 
                Arc::new(RwLock::new(KdIndex::new(field_vec.len(), index_name.to_string(), field_vec.clone(), schema.attributes.len()))));
                hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.data_into_tree(&mut bulk_load_data[..]);
                //hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.print_tree();
            },
            "R" => {        hf.r_index_map.write().unwrap().insert(index_name.to_string(), 
                Arc::new(RwLock::new(RIndex::new(field_vec.len(), index_name.to_string(), field_vec.clone(), schema.attributes.len()))));
                hf.r_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.data_into_tree(&mut bulk_load_data[..]);
                //hf.r_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.print_tree();
            },
            _ => {error!("CreateIndex Tree Type Command not supported");},
        }
    }

    fn use_index_equal (&self, tree_type: &str, index_name: &str, container_id: ContainerId, attributes: &str, table: &Table) -> Vec<Tuple> {
        debug!("Comes to use_index_equal in Storage Manager");
        let hf_map = &self.hf_map.read().unwrap();
        let hf = hf_map.get(&container_id).unwrap();
        let schema = &table.schema;
        let attribute_vals = StorageManager::get_attribute_list(attributes);
        let mut field_vec = Vec::new(); //LOTS OF ERROR CHECKING
        let mut i = 0;
        let mut idx_fields = Vec::new();
        match tree_type {
            "KD" => { idx_fields = hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.idx_fields.clone();},
            "R" => {idx_fields = hf.r_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.get_idx_fields().clone();},
            _ => {error!("UseIndex Tree Type Command not supported");},
        }
        for attribute_val in attribute_vals {
            match &schema.get_attribute(idx_fields[i]).unwrap().dtype {
                Int => {field_vec.push(Field::IntField(attribute_val.parse::<i32>().unwrap()))},
                String => {field_vec.push(Field::StringField(attribute_val))},
            }
            i += 1;
        }
        let mut res = Vec::new();
        match tree_type {
            "KD" => {res = hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.get(&field_vec);},
            "R" => {res = hf.r_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.get(&field_vec);},
            _ => {error!("UseIndex Tree Type Command not supported");},
        }        
        //KdTree::print_vec(&res);
        return KdTree::vec_field_to_tuple(&res);
    }

    fn use_index_range(&self, tree_type: &str, index_name: &str, container_id: ContainerId, attributes: &str, table: &Table) -> Vec<Tuple> {
        debug!("Comes to use_index_range in Storage Manager");
        let hf_map = &self.hf_map.read().unwrap();
        let hf = hf_map.get(&container_id).unwrap();
        let schema = &table.schema;
        let mut tokens = attributes.split(";");
        let mut j = 0;
        let mut min = Vec::new();
        let mut max = Vec::new();
        let mut idx_fields = Vec::new();
        match tree_type {
            "KD" => { idx_fields = hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.idx_fields.clone();},
            "R" => {idx_fields = hf.r_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.get_idx_fields().clone();},
            _ => {error!("UseIndex Tree Type Command not supported");},
        }
        while let Some(attribute_min_max_val) = tokens.next() {
            let min_max_val_list = StorageManager::get_attribute_list(attribute_min_max_val);
            for i in 0..min_max_val_list.len() {
                match &schema.get_attribute(idx_fields[i]).unwrap().dtype {
                    Int => {
                            if j == 0 {
                                min.push(Field::IntField(min_max_val_list[i].parse::<i32>().unwrap()));
                            }
                            else {
                                max.push(Field::IntField(min_max_val_list[i].parse::<i32>().unwrap()));
                            }
                        },
                    String => {
                        if j == 0 {
                            min.push(Field::StringField(min_max_val_list[i].to_string()));
                        }
                        else {
                            max.push(Field::StringField(min_max_val_list[i].to_string()));
                        }
                    }
                }
            }     
            j += 1;     
        }
        let mut res = Vec::new();
        match tree_type {
            "KD" => {
                res = hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.range_query(&min, &max);},
            "R" => {     
                let hf_iterator = self.get_iterator(container_id, TransactionId::new(), Permissions::ReadOnly);   
                let padded_min = KdTree::padding2(&min, &idx_fields, schema.attributes.len());
                let padded_max = KdTree::padding2(&max, &idx_fields, schema.attributes.len());
                // debug!("min is {:?}", padded_min);
                // debug!("max is {:?}", padded_max);
                for (i, val) in hf_iterator.enumerate() {
                    let tuple = Tuple::from_bytes(&val);
                    if StorageManager::scan_tuple_for_range(&tuple, &padded_min, &padded_max, &idx_fields) {
                        res.push(tuple.field_vals.clone());
                    }
                }
            },
            _ => {error!("UseIndex Tree Type Command not supported");},
        }
        return KdTree::vec_field_to_tuple(&res);
    }

    fn use_index_knn(&self, tree_type: &str, index_name: &str, container_id: ContainerId, attributes: &str, _k: Option<&str>, table: &Table) -> Vec<Tuple> {
        debug!("Comes to use_index_knn in Storage Manager");
        let hf_map = &self.hf_map.read().unwrap();
        let hf = hf_map.get(&container_id).unwrap();
        let schema = &table.schema;
        if _k.is_none() {
            error!("no k specified");
        }
        let k = _k.unwrap().parse::<usize>().unwrap();
        let attribute_vals = StorageManager::get_attribute_list(attributes);
        let mut field_vec = Vec::new(); //LOTS OF ERROR CHECKING
        let mut i = 0;
        let mut idx_fields = Vec::new();
        match tree_type {
            "KD" => { idx_fields = hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.idx_fields.clone();},
            "R" => {idx_fields = hf.r_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.get_idx_fields().clone();},
            _ => {error!("UseIndex Tree Type Command not supported");},
        }
        for attribute_val in attribute_vals {
            match &schema.get_attribute(idx_fields[i]).unwrap().dtype {
                Int => {field_vec.push(Field::IntField(attribute_val.parse::<i32>().unwrap()))},
                String => {field_vec.push(Field::StringField(attribute_val))},
            }
            i += 1;
        }
        debug!("field vec is {:?}", &field_vec);
        let mut res = Vec::new();
        match tree_type {
            "KD" => {res = hf.kd_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.knn(&field_vec, k);},
            "R" => {res = hf.r_index_map.write().unwrap().get(index_name).unwrap().write().unwrap().tree.knn(&field_vec, k);},
            _ => {error!("UseIndex Tree Type Command not supported");},
        }      
        //KdTree::print_vec(&res);
        return KdTree::vec_field_to_tuple(&res);
    }

    pub fn use_index_by_id(&self, tree_type: &str, query_type: &str, index_name: &str, container_id: ContainerId, attributes: &str, _k: Option<&str>, table: &Table) -> Vec<Tuple> {
        debug!("Comes to use_index_by_id in Storage Manager");
        match query_type {
            "RANGE" => {self.use_index_range(tree_type, index_name, container_id, attributes, table)},
            "EQ" => {self.use_index_equal(tree_type, index_name, container_id, attributes, table)},
            "KNN" => {self.use_index_knn(tree_type, index_name, container_id, attributes, _k, table)},
            _ => {error!("UseIndex command not supported"); Vec::new()},
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        let container_dir = format!("{}/containers/", &storage_path);
        fs::create_dir_all(container_dir).expect("Can't create sm container directory");
        let mut hf_serialized_map = HashMap::new();
        let mut hf_map = HashMap::new();
        // Create a directory that holds information on containers and its location
        let container_location_dir = format!("{}/containers_location/", &storage_path);
        if Path::new(&container_location_dir).exists() {
            let locations = fs::read_dir(&container_location_dir).unwrap();
            for location in locations {
                let location = location.unwrap();
                let location_path = location.path();
                let container_id = location
                    .file_name()
                    .into_string()
                    .unwrap()
                    .parse::<u16>()
                    .unwrap();
                let reader = File::open(&location_path).expect("error opening file");
                let serialized_hf: SerializedHeapFile =
                    serde_json::from_reader(reader).expect("error reading from json");
                hf_serialized_map.insert(container_id, serialized_hf);
                let heap_file_path = hf_serialized_map
                    .get(&container_id)
                    .unwrap()
                    .hf_path
                    .read()
                    .unwrap();
                let heap_file = HeapFile::new(heap_file_path.to_path_buf()).unwrap();
                hf_map.insert(container_id, heap_file);
            }
        }
        StorageManager {
            hf_serialized_map: Arc::new(RwLock::new(hf_serialized_map)),
            hf_map: Arc::new(RwLock::new(hf_map)),
            storage_path,
            is_temp: false,
        }
    }

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        debug!("Making new temp storage_manager {}", storage_path);
        StorageManager::new(storage_path)
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        let container_num_page = self.get_num_pages(container_id);
        for i in 0..container_num_page {
            let page = &mut self
                .get_page(container_id, i, tid, Permissions::ReadWrite, false)
                .unwrap();
            match &page.add_value(&value) {
                None => continue,
                Some(slot_id) => {
                    //self.write_data_to_file(container_id, page, i, *slot_id, value);
                    self.write_updated_page_to_file(container_id, page, i)
                        .expect("Can't write updated page to file");
                    return ValueId {
                        container_id,
                        segment_id: None,
                        page_id: Some(i),
                        slot_id: Some(*slot_id),
                    };
                }
            }
        }
        let mut new_page = Page::new(container_num_page);
        let slot_id = &new_page.add_value(&value).unwrap();
        self.write_page(container_id, new_page, tid)
            .expect("Can't write new page to file");
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(self.get_num_pages(container_id) - 1),
            slot_id: Some(*slot_id),
        }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut res = Vec::new();
        for value in values {
            res.push(self.insert_value(container_id, value, tid));
        }
        res
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let page = &mut self
            .get_page(
                id.container_id,
                id.page_id.unwrap(),
                tid,
                Permissions::ReadWrite,
                false,
            )
            .unwrap();
        match &page.delete_value(id.slot_id.unwrap()) {
            None => Ok(()),
            Some(_) => {
                self.write_updated_page_to_file(id.container_id, page, id.page_id.unwrap())
                    .expect("Can't write updated data to file");
                Ok(())
            }
        }
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        self.delete_value(id, _tid).expect("Can't delete value");
        Ok(self.insert_value(id.container_id, value, _tid))
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        let parent_filepath = format!("{}/containers", self.storage_path);
        let child_filename = format!("{}/{}", parent_filepath, container_id.to_string());
        let child_filepath = Path::new(&child_filename);
        self.hf_map.write().unwrap().insert(
            container_id,
            HeapFile::new(child_filepath.to_path_buf()).unwrap(),
        );
        self.hf_serialized_map.write().unwrap().insert(
            container_id,
            SerializedHeapFile {
                hf_path: Arc::new(RwLock::new(child_filepath.to_path_buf())),
            },
        );
        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let hf_serialized_map = &self.hf_serialized_map.read().unwrap();
        let serialized_hf = hf_serialized_map.get(&container_id).unwrap();
        let hf_filepath = serialized_hf.hf_path.read().unwrap().clone();
        self.hf_map.write().unwrap().remove(&container_id);
        self.hf_serialized_map
            .write()
            .unwrap()
            .remove(&container_id);
        fs::remove_file(&hf_filepath).expect("Can't remove container");
        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let hf_serialized_map = &self.hf_serialized_map.read().unwrap();
        let hf_serialized = hf_serialized_map.get(&container_id).unwrap();
        let hf_file_path = hf_serialized.hf_path.read().unwrap().to_path_buf();
        HeapFileIterator::new(
            container_id,
            tid,
            Arc::new(HeapFile::new(hf_file_path).unwrap()),
        )
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        match &self.hf_map.read().unwrap().get(&id.container_id) {
            // if container exists
            None => Err(CrustyError::CrustyError(String::from(
                "Couldn't find Container",
            ))),
            Some(heap_file) => {
                match heap_file.read_page_from_file(id.page_id.unwrap()) {
                    // if read page succeeds
                    Err(_) => Err(CrustyError::CrustyError(String::from("Couldn't find page"))),
                    Ok(page) => match page.get_value(id.slot_id.unwrap()) {
                        None => Err(CrustyError::CrustyError(String::from(
                            "Couldn't find record",
                        ))),
                        Some(value) => Ok(value),
                    },
                }
            }
        }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, _tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        if Path::new(&self.storage_path).exists() {
            fs::remove_dir_all(&self.storage_path);
        }
        fs::create_dir_all(&self.storage_path).expect("Can't create sm directory");
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    fn clear_cache(&self) {
        panic!("TODO milestone hs");
    }

    /// Shutdown the storage manager. Can call drop. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    /// If not a temp SM, this should serialize the mapping between containerID and Heapfile.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        println!("COMES TO SHUTDOWN");
        info!("Shutting down");
        if self.is_temp {
            self.reset().expect("Can't reset");
            return;
        }
        let filepath = format!("{}/containers_location", &self.storage_path);
        fs::create_dir_all(&filepath).expect("Can't create sm directory");
        println!("PASS SHUTDOWN");
        for (container_id, serialized_hf) in self.hf_serialized_map.read().unwrap().iter() {
            let name = container_id.to_string();
            let filename = format!("{}/{}", filepath, name);
            serde_json::to_writer(
                fs::File::create(filename).expect("error creating file"),
                &serialized_hf,
            )
            .expect("error deserializing storage manager");
        }
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.get_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    /// Shutdown the storage manager. Can call be called by shutdown. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    fn drop(&mut self) {
        info!("Shutting down");
        if self.is_temp {
            self.reset().expect("Can't reset");
            return;
        }
        let filepath = format!("{}/containers_location", self.storage_path);
        fs::create_dir_all(&filepath).expect("Can't create sm directory");
        for (container_id, serialized_hf) in self.hf_serialized_map.read().unwrap().iter() {
            let name = container_id.to_string();
            let filename = format!("{}/{}", filepath, name);
            serde_json::to_writer(
                fs::File::create(filename).expect("error creating file"),
                &serialized_hf,
            )
            .expect("error deserializing storage manager");
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.get_bytes()[..], p2.get_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();
        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
