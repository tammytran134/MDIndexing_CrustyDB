extern crate log;

use crate::catalog;
use crate::ids::{ContainerId, StateType, CONTAINER_COUNTER};
use crate::prelude::*;
use crate::table::*;
use catalog::Catalog;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::fs::File;

/// The actual database.
#[derive(Clone, Serialize, Deserialize)]
pub struct Database {
    /// Name of the database.
    pub name: String,
    // Requires RwLock on both map and tables to enable adding/removing tables as well as table mutability.
    // TODO: can likely remove RwLock on table because all modifications to Table solely occur within the HeapFile.
    /// Locks for the tables.
    // #[serde(skip)]
    pub tables: Arc<RwLock<HashMap<ContainerId, Arc<RwLock<Table>>>>>,
    // #[serde(skip)]
    pub named_containers: Arc<RwLock<HashMap<ContainerId, (String, StateType)>>>,
}

impl Database {
    /// Initialize a new database with a given name.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for the new database.
    pub fn new(name: String) -> Self {
        Database {
            name,
            tables: Arc::new(RwLock::new(HashMap::new())),
            named_containers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn load(filename: PathBuf) -> Self {
        debug!("Loading database from file {}", filename.display());
        let reader = File::open(&filename).expect("error opening file");
        serde_json::from_reader(reader).expect("error reading from json")
    }
}

impl Catalog for Database {
    /// Gets the tables from the catalog of the database.
    fn get_tables(&self) -> Arc<RwLock<HashMap<ContainerId, Arc<RwLock<Table>>>>> {
        self.tables.clone()
    }

    fn get_table_id(&self, name: &str) -> Option<ContainerId> {
        //TODO mixed usage of &str and &String. for code that had &str it was coded as &x.to_string()
        let containers = self.named_containers.read().unwrap();
        for (id, c) in containers.iter() {
            if let StateType::BaseTable = c.1 {
                if *name == c.0 {
                    return Some(*id);
                }
            }
        }
        debug!("Unable to find table `{}`. Named Containers: {:?}", name, containers);
        None
    }

    /// Register and create new state to store in the SM
    ///
    fn get_new_container_id(
        &self,
        state_type: StateType,
        name: Option<String>,
    ) -> Result<ContainerId, CrustyError> {
        if let StateType::BaseTable = state_type {
            match name.clone() {
                Some(name) => {
                    if self.get_table_id(&name).is_some() {
                        return Err(CrustyError::CrustyError(String::from(
                            "database already has a table with this name",
                        )));
                    }
                }
                None => {
                    return Err(CrustyError::CrustyError(String::from(
                        "base tables must have name",
                    )))
                }
            }
        }
        let new_cid = CONTAINER_COUNTER.fetch_add(1, Ordering::SeqCst);
        if let Some(n) = name {
            //Save the cid if this has a name
            let mut containers = self.named_containers.write().unwrap();
            containers.insert(new_cid, (n, state_type));
        }
        Ok(new_cid)
    }
}

//TODO: Add catalog unit testing
