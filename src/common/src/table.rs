use crate::TableSchema;

/// Table implementation.
#[derive(Serialize, Deserialize, Clone)]
pub struct Table {
    /// Table name.
    pub name: String,
    /// Table schema.
    pub schema: TableSchema,
}

impl Table {
    /// Creates a new table with the given name and heapfile.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of table.
    /// * `file` - HeapFile of the table.
    pub fn new(name: String, schema: TableSchema) -> Self {
        Table { name, schema }
    }
}
