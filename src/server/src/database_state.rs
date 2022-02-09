use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::{StorageManager, StorageTrait};
use common::catalog::Catalog;
use common::database::Database;
use common::ids::{AtomicTimeStamp, StateMeta, StateType};
use common::physical_plan::PhysicalPlan;
use common::prelude::*;
use common::table::Table;
use common::{get_attr, Attribute, QueryResult};
use sqlparser::ast::ColumnDef;
use sqlparser::ast::TableConstraint;

use crate::query_registrar::QueryRegistrar;
use crate::sql_parser::{ParserResponse, SQLParser};

use std::sync::atomic::AtomicU32;

#[derive(Serialize)]
pub struct DatabaseState {
    pub id: u64,
    pub name: String,
    // pub catalog: Catalog,
    pub database: Database,

    #[serde(skip_serializing)]
    pub storage_manager: &'static StorageManager,

    #[serde(skip_serializing)]
    // runtime information
    pub active_client_connections: RwLock<HashSet<u64>>,

    // The list of things stored
    container_vec: Arc<RwLock<HashMap<ContainerId, StateMeta>>>,

    // Time for operations based on timing (typically inserts)
    pub atomic_time: AtomicTimeStamp,

    #[serde(skip_serializing)]
    query_registrar: QueryRegistrar,
}

#[allow(dead_code)]
impl DatabaseState {
    // initializing within here
    pub fn new_from_path(
        path: PathBuf,
        _storage_path: String,
        sm: &'static StorageManager,
    ) -> Result<Self, CrustyError> {
        debug!("Creating new DBState from path {:?}", path);
        // TODO: Remove magic numbers to parse out db json file name.
        let cand = path.display().to_string();
        // FIXME: that 11 hard-coded there....
        let cand_name = &cand[11..cand.len() - 5];
        debug!("cand: {} cand_name {}", cand, cand_name);

        match fs::File::open(cand.clone()) {
            Ok(res) => {
                let db_name = cand_name.to_string();
                let db_id = DatabaseState::get_database_id(&db_name);

                let database = DatabaseState::load_database_from_file(res, sm)?;
                let _db_state = DatabaseState {
                    id: db_id,
                    name: db_name,
                    database,
                    storage_manager: sm,
                    active_client_connections: RwLock::new(HashSet::new()),
                    container_vec: Arc::new(RwLock::new(HashMap::new())),
                    atomic_time: AtomicTimeStamp::new(0),
                    query_registrar: QueryRegistrar::new(),
                };
                panic!("Fix container meta loading"); // TODO
                                                      //Ok(db_state)
            }
            _ => Err(CrustyError::IOError(String::from("Failed to open db file"))),
        }
    }

    pub fn get_database_id(db_name: &str) -> u64 {
        let mut s = DefaultHasher::new();
        db_name.hash(&mut s);
        s.finish()
    }

    pub fn new_from_name(db_name: &str, sm: &'static StorageManager) -> Result<Self, CrustyError> {
        let db_name: String = String::from(db_name);
        let db_id = DatabaseState::get_database_id(&db_name);
        debug!(
            "Creating new DatabaseState; name: {} id: {}",
            db_name, db_id
        );
        let database = Database::new(db_name.to_string());

        let db_state = DatabaseState {
            id: db_id,
            name: db_name,
            database,
            storage_manager: sm,
            active_client_connections: RwLock::new(HashSet::new()),
            container_vec: Arc::new(RwLock::new(HashMap::new())),
            atomic_time: AtomicU32::new(0),
            query_registrar: QueryRegistrar::new(),
        };
        Ok(db_state)
    }

    pub fn load(filename: PathBuf, sm: &'static StorageManager) -> Result<Self, CrustyError> {
        let database:Database = Database::load(filename);
        let db_name: String = database.name.clone();
        let db_id = DatabaseState::get_database_id(&db_name);
        debug!(
            "Loading DatabaseState; name: {} id: {}",
            db_name, db_id
        );

        let db_state = DatabaseState {
            id: db_id,
            name: db_name,
            database,
            storage_manager: sm,
            active_client_connections: RwLock::new(HashSet::new()),
            container_vec: Arc::new(RwLock::new(HashMap::new())),
            atomic_time: AtomicU32::new(0),
            query_registrar: QueryRegistrar::new(),
        };
        Ok(db_state)
    }

    pub fn register_new_client_connection(&self, client_id: u64) {
        debug!(
            "Registering new client connection: {:?} to database: {:?}",
            client_id, self.id
        );
        self.active_client_connections
            .write()
            .unwrap()
            .insert(client_id);
    }

    pub fn get_current_time(&self) -> LogicalTimeStamp {
        self.atomic_time.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn close_client_connection(&self, client_id: u64, metadata_path: String) {
        info!("Closing client connection: {:?}...", &client_id);
        // Remove client from this db
        self.active_client_connections
            .write()
            .unwrap()
            .remove(&client_id);
        // Check if that was the last client connected to this DB
        if self.active_client_connections.read().unwrap().is_empty() {
            // Construct path where db will be persisted
            let mut persist_path = metadata_path.clone();
            persist_path.push_str(&self.name);
            persist_path.push_str(".json");
            // Serialize DB into a string and write it to the path
            if let Ok(s) = serde_json::to_string(&self) {
                info!("Persisting db on: {:?}", &metadata_path);
                fs::write(&persist_path, s).expect("Failed to write out db json");
            }
        }
        info!("Closing client connection: {:?}...DONE", &client_id);
    }

    pub fn get_table_names(&self) -> Result<String, CrustyError> {
        let mut table_names = Vec::new();
        {
            let tables = self.database.get_tables();
            let tables_ref = tables.read().unwrap();
            for table in tables_ref.values() {
                let name = table.read().unwrap().name.clone();
                table_names.push(name);
            }
        }
        let table_names = table_names.join("\n");
        if table_names.is_empty() {
            Ok(String::from("No tables"))
        } else {
            Ok(table_names)
        }
    }

    pub fn get_registered_query_names(&self) -> Result<String, CrustyError> {
        self.query_registrar.get_registered_query_names()
    }

    /// Load in database.
    ///
    /// # Arguments
    ///
    /// * `db` - Name of database to load in.
    /// * `id` - Thread id to get the lock.
    pub fn load_database_from_file(
        file: fs::File,
        storage_manager: &StorageManager,
    ) -> Result<Database, CrustyError> {
        debug!("Loading DB from file {:?}", file);
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents)?;
        let db_content_str: &str = &contents;
        let db_cand: Database = serde_json::from_str(db_content_str).unwrap();
        {
            let mut tables_ref = db_cand.tables.write().unwrap();
            for table_ref in tables_ref.values_mut() {
                let table = table_ref.read().unwrap();

                debug!("Loading table: {:?}", table.name.clone());

                error!("TODO get table / container ID");
                storage_manager.create_container(
                    0,
                    StorageManager::get_simple_config(),
                    Some(table.name.clone()),
                    common::ids::StateType::BaseTable,
                    None,
                )?;
            }
        }
        Ok(db_cand)
    }

    /// Creates a new table.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the new table.
    /// * `cols` - Table columns.
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<QueryResult, CrustyError> {
        // Constraints aren't implemented yet

        let db = &self.database;
        let mut tables_ref = db.tables.write().unwrap();
        let table_id =
            db.get_new_container_id(StateType::BaseTable, Some(table_name.to_string()))?;
        let pks = match SQLParser::get_pks(columns, constraints) {
            Ok(pks) => pks,
            Err(ParserResponse::SQLConstraintError(s)) => return Err(CrustyError::CrustyError(s)),
            _ => unreachable!(),
        };

        let mut attributes: Vec<Attribute> = Vec::new();
        for col in columns {
            let constraint = if pks.contains(&col.name) {
                common::Constraint::PrimaryKey
            } else {
                common::Constraint::None
            };
            let attr = Attribute {
                name: col.name.value.clone(),
                dtype: get_attr(&col.data_type)?,
                constraint,
            };
            attributes.push(attr);
        }
        let schema = TableSchema::new(attributes);
        debug!("Creating table with schema: {:?}", schema);

        let table = Table::new(table_name.to_string(), schema);
        self.storage_manager.create_container(
            table_id,
            StorageManager::get_simple_config(),
            Some(table_name.to_string()),
            common::ids::StateType::BaseTable,
            None,
        )?;
        tables_ref.insert(table_id, Arc::new(RwLock::new(table)));
        Ok(QueryResult::new(&format!("Table {} created", table_name)))
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        self.query_registrar.reset()?;
        let mut conns = self.active_client_connections.write().unwrap();
        conns.clear();
        drop(conns);
        let mut containers = self.container_vec.write().unwrap();
        containers.clear();
        drop(containers);
        Ok(())
    }

    /// Register a new query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query name to register.
    /// * `query_plan` - Query plan to register.
    pub fn register_query(
        &self,
        query_name: String,
        json_path: String,
        query_plan: Arc<PhysicalPlan>,
    ) -> Result<(), CrustyError> {
        self.query_registrar
            .register_query(query_name, json_path, query_plan)
    }

    /// Update metadata for beginning to run a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    /// * `start_timestamp` - Optional start timestamp.
    /// * `end_timestamp` - End timestamp.
    pub fn begin_query(
        &self,
        query_name: &str,
        start_timestamp: Option<LogicalTimeStamp>,
        end_timestamp: LogicalTimeStamp,
    ) -> Result<Arc<PhysicalPlan>, CrustyError> {
        self.query_registrar
            .begin_query(query_name, start_timestamp, end_timestamp)
    }

    /// Update metadata at end of a query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    pub fn finish_query(&self, query_name: &str) -> Result<(), CrustyError> {
        self.query_registrar.finish_query(query_name)
    }
}
