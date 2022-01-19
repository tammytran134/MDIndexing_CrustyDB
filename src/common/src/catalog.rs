use crate::ids::StateType;
use crate::prelude::*;
use crate::table::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Functions needed to implement a catalog. It keeps track of all available tables in the database and their associated schemas.
pub trait Catalog {
    /// Get tables from catalog.
    fn get_tables(&self) -> Arc<RwLock<HashMap<ContainerId, Arc<RwLock<Table>>>>>;

    /// Get the table ID from the state catalog
    fn get_table_id(&self, name: &str) -> Option<ContainerId>;

    /// Allocate a new container ID. Store the reference to this by name
    fn get_new_container_id(
        &self,
        state_type: StateType,
        name: Option<String>,
    ) -> Result<ContainerId, CrustyError>;

    /// Get the table pointer for the catalog.
    ///
    /// # Arguments
    ///
    /// * `table_id` - Id of table to get the pointer for.
    fn get_table_ptr(&self, table_id: ContainerId) -> Result<Arc<RwLock<Table>>, CrustyError> {
        let tables = self.get_tables();
        let tables_ref: &HashMap<ContainerId, Arc<RwLock<Table>>> = &tables.read().unwrap();
        match tables_ref.get(&table_id) {
            Some(table_ptr) => Ok(Arc::clone(table_ptr)),
            _ => Err(CrustyError::CrustyError(String::from("Table not found"))),
        }
    }

    /// Checks if the table id is valid in the catalog.
    ///
    /// # Arguments
    ///
    /// * `table_id` - Id of table to check if it is valid.
    fn is_valid_table(&self, table_id: ContainerId) -> bool {
        let tables = self.get_tables();
        let tables_ref: &HashMap<ContainerId, Arc<RwLock<Table>>> = &tables.read().unwrap();
        matches!(tables_ref.get(&table_id), Some(_))
    }

    /// Checks if the column is valid for the given table.
    ///
    /// # Arguments
    ///
    /// * `table_id` - Id of table to look for the column name in.
    /// * `col_name` - Name of column to look for in the table.
    fn is_valid_column(&self, table_id: ContainerId, col_name: &str) -> bool {
        let tables = self.get_tables();
        let tables_ref: &HashMap<ContainerId, Arc<RwLock<Table>>> = &tables.read().unwrap();
        match tables_ref.get(&table_id) {
            Some(table_ptr) => {
                let table_ref = table_ptr.read().unwrap();
                table_ref.schema.get_field_index(col_name).is_some()
            }
            _ => false,
        }
    }

    /// Gets the table schema from the catalog.
    ///
    /// # Arguments
    ///
    /// * `table_id` - Id of table to get the schema for.
    fn get_table_schema(&self, table_id: ContainerId) -> Result<TableSchema, CrustyError> {
        let tables = self.get_tables();
        let tables_ref: &HashMap<ContainerId, Arc<RwLock<Table>>> = &tables.read().unwrap();
        match tables_ref.get(&table_id) {
            Some(table_ptr) => {
                let table = table_ptr.read().unwrap();
                Ok(table.schema.clone())
            }
            _ => Err(CrustyError::CrustyError(String::from("Table not found"))),
        }
    }

    /// Gets the table name from the catalog.
    ///
    /// # Arguments
    ///
    /// * `table_id` - Id of table to get the name for.
    fn get_table_name(&self, table_id: ContainerId) -> Result<String, CrustyError> {
        let tables = self.get_tables();
        let tables_ref: &HashMap<ContainerId, Arc<RwLock<Table>>> = &tables.read().unwrap();
        match tables_ref.get(&table_id) {
            Some(table_ptr) => {
                let table = table_ptr.read().unwrap();
                Ok(table.name.clone())
            }
            _ => Err(CrustyError::CrustyError(String::from("Table not found"))),
        }
    }
}
