extern crate csv;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

// use itertools::Itertools;
use delta_storage_trait::ValueDiff;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use sqlparser::ast;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
// use proc_macro::bridge::client::ProcMacro::Attr;

pub mod catalog;
pub mod commands;
pub mod crusty_graph;
pub mod database;
pub mod delta_storage_trait;
pub mod ids;
pub mod logical_plan;
pub use logical_plan::{AggOp, SimplePredicateOp};
pub mod physical_plan;
pub mod storage_trait;
pub mod table;
pub mod testutil;

/// How big each page is
pub const PAGE_SIZE: usize = 4096;
// How many pages a buffer pool can hold
pub const PAGE_SLOTS: usize = 50;
// Default method for how to retun string results
pub const QUERY_RESULT_TYPE: QueryResultType = QueryResultType::CSV(false); //QueryResultType::WIDTH(10);

pub mod prelude {
    pub use crate::ids::Permissions;
    pub use crate::ids::{
        ContainerId, LogicalTimeStamp, PageId, SlotId, StateType, TransactionId, ValueId,
    };
    pub use crate::table::Table;
    pub use crate::CrustyError;
    pub use crate::{DataType, Field, TableSchema, Tuple};
}

/// Custom error type.
#[derive(Debug, Clone, PartialEq)]
pub enum CrustyError {
    /// IO Errors.
    IOError(String),
    /// Custom errors.
    CrustyError(String),
    /// Validation errors.
    ValidationError(String),
    /// Execution errors.
    ExecutionError(String),
    /// Transaction aborted.
    TransactionAbortedError,
}

impl fmt::Display for CrustyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                CrustyError::ValidationError(s) => format!("Validation Error: {}", s),
                CrustyError::ExecutionError(s) => format!("Execution Error: {}", s),
                CrustyError::CrustyError(s) => format!("Crusty Error: {}", s),
                CrustyError::IOError(s) => s.to_string(),
                CrustyError::TransactionAbortedError => String::from("Transaction Aborted Error"),
            }
        )
    }
}

// Implement std::convert::From for AppError; from io::Error
impl From<io::Error> for CrustyError {
    fn from(error: io::Error) -> Self {
        CrustyError::IOError(error.to_string())
    }
}

impl Error for CrustyError {}

/// Return type for a query result.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct QueryResult {
    pub result: String,
}

impl QueryResult {
    /// Return an empty result.
    pub fn empty() -> Self {
        Self {
            result: String::from(""),
        }
    }

    /// Return a result with string.
    ///
    /// # Arguments
    ///
    /// * `result` - Result to return.
    pub fn new(result: &str) -> Self {
        Self {
            result: result.to_string(),
        }
    }

    /// Get the result.
    pub fn result(&self) -> &str {
        &self.result
    }
}

/// Handle schemas.
#[derive(PartialEq, Clone, Debug)]
pub struct TableSchema {
    /// Attributes of the schema.
    attributes: Vec<Attribute>,
    /// Mapping from attribute name to order in the schema.
    name_map: HashMap<String, usize>,
}

impl Serialize for TableSchema {
    /// Custom serialize to avoid serializing name_map.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.attributes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableSchema {
    /// Custom deserialize to avoid serializing name_map.
    fn deserialize<D>(deserializer: D) -> Result<TableSchema, D::Error>
    where
        D: Deserializer<'de>,
    {
        let attrs = Vec::deserialize(deserializer)?;
        Ok(TableSchema::new(attrs))
    }
}

impl TableSchema {
    /// Create a new schema.
    ///
    /// # Arguments
    ///
    /// * `attributes` - Attributes of the schema in the order that they are in the schema.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        let mut name_map = HashMap::new();
        for (i, attr) in attributes.iter().enumerate() {
            name_map.insert(attr.name().to_string(), i);
        }
        Self {
            attributes,
            name_map,
        }
    }

    /// Create a new schema with the given names and dtypes.
    ///
    /// # Arguments
    ///
    /// * `names` - Names of the new schema.
    /// * `dtypes` - Dypes of the new schema.
    pub fn from_vecs(names: Vec<&str>, dtypes: Vec<DataType>) -> Self {
        let mut attrs = Vec::new();
        for (name, dtype) in names.iter().zip(dtypes.iter()) {
            attrs.push(Attribute::new(name.to_string(), dtype.clone()));
        }
        TableSchema::new(attrs)
    }

    /// Get the attribute from the given index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the attribute to look for.
    pub fn get_attribute(&self, i: usize) -> Option<&Attribute> {
        self.attributes.get(i)
    }

    /// Get the index of the attribute.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to get the index for.
    pub fn get_field_index(&self, name: &str) -> Option<&usize> {
        self.name_map.get(name)
    }

    /// Returns attribute(s) that are primary keys
    ///
    ///
    pub fn get_pks(&self) -> Vec<Attribute> {
        let mut pk_attributes: Vec<Attribute> = Vec::new();
        for attribute in &self.attributes {
            if attribute.constraint == Constraint::PrimaryKey {
                pk_attributes.push(attribute.clone());
            }
        }
        pk_attributes
    }

    /// Check if the attribute name is in the schema.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to look for.
    pub fn contains(&self, name: &str) -> bool {
        self.name_map.contains_key(name)
    }

    /// Get an iterator of the attributes.
    pub fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attributes.iter()
    }

    /// Merge two schemas into one.
    ///
    /// The other schema is appended to the current schema.
    ///
    /// # Arguments
    ///
    /// * `other` - Other schema to add to current schema.
    pub fn merge(&self, other: &Self) -> Self {
        let mut attrs = self.attributes.clone();
        attrs.append(&mut other.attributes.clone());
        Self::new(attrs)
    }

    /// Returns the length of the schema.
    pub fn size(&self) -> usize {
        self.attributes.len()
    }

    /// Returns the size of the schema in bytes.
    pub fn byte_size(&self) -> usize {
        let mut total: usize = 0;
        for attr in self.attributes.iter() {
            total += attr.get_byte_len();
        }
        total
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum Constraint {
    None,
    PrimaryKey,
    Unique,
    NotNull,
    UniqueNotNull,
    ForeignKey(prelude::ContainerId), // Points to other table. Infer PK
    NotNullFKey(prelude::ContainerId),
}

/// Handle attributes. Pairs the name with the dtype.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Attribute {
    /// Attribute name.
    pub name: String,
    /// Attribute dtype.
    pub dtype: DataType,
    /// Attribute constraint
    pub constraint: Constraint,
}

impl Attribute {
    /// Create a new attribute with the given name and dtype.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute.
    /// * `dtype` - Dtype of the attribute.
    // pub fn new(name: String, dtype: DataType) -> Self { Self { name, dtype, is_pk: false } }

    pub fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::None,
        }
    }

    pub fn new_with_constraint(name: String, dtype: DataType, constraint: Constraint) -> Self {
        Self {
            name,
            dtype,
            constraint,
        }
    }

    pub fn new_pk(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::PrimaryKey,
        }
    }

    /// Returns the name of the attribute.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dtype of the attribute.
    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }

    // TODO(williamma12): Where does the 132 come from?
    /// Returns the length of the dtype in bytes.
    pub fn get_byte_len(&self) -> usize {
        match self.dtype {
            DataType::Int => 4,
            DataType::String => 132,
        }
    }
}

/// Enumerate the supported dtypes.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub enum DataType {
    Int,
    String,
}

/// For each of the dtypes, make sure that there is a corresponding field type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord, Clone, Hash)]
pub enum Field {
    IntField(i32),
    StringField(String),
}

impl Field {
    /// Function to convert a Tuple field into bytes for serialization
    ///
    /// This function always uses least endian byte ordering and stores strings in the format |string length|string contents|.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Field::IntField(x) => x.to_le_bytes().to_vec(),
            Field::StringField(s) => {
                let s_len: usize = s.len();
                let mut result = s_len.to_le_bytes().to_vec();
                let mut s_bytes = s.clone().into_bytes();
                let padding_len: usize = 128 - s_bytes.len();
                let pad = vec![0; padding_len];
                s_bytes.extend(&pad);
                result.extend(s_bytes);
                result
            }
        }
    }

    pub fn copy_field(&self) -> Self {
        let field_copy;
        match self {
            Field::IntField(i) => {field_copy = Field::IntField(*i)},
            Field::StringField(s) => {field_copy = Field::StringField(s.to_string())},
        } 
        field_copy      
    }

    /// Unwraps integer fields.
    pub fn unwrap_int_field(&self) -> i32 {
        match self {
            Field::IntField(i) => *i,
            _ => panic!("Expected i32"),
        }
    }

    /// Unwraps string fields.
    pub fn unwrap_string_field(&self) -> &str {
        match self {
            Field::StringField(s) => s,
            _ => panic!("Expected String"),
        }
    }

    pub fn is_int_field(&self) -> bool {
        match self {
            Field::IntField(_) => true,
            Field::StringField(_) => false,
        }         
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Field::IntField(x) => write!(f, "{}", x),
            Field::StringField(x) => write!(f, "{}", x),
        }
    }
}

/// Tuple type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Hash)]
pub struct Tuple {
    /// Tuple data.
    pub field_vals: Vec<Field>,
}

impl Tuple {
    /// Create a new tuple with the given data.
    ///
    /// # Arguments
    ///
    /// * `field_vals` - Field values of the tuple.
    pub fn new(field_vals: Vec<Field>) -> Self {
        Self { field_vals }
    }

    /// Get the field at index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the field.
    pub fn get_field(&self, i: usize) -> Option<&Field> {
        self.field_vals.get(i)
    }

    /// Update the index at field.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the value to insert.
    /// * `f` - Value to add.
    ///
    /// # Panics
    ///
    /// Panics if the index is out-of-bounds.
    pub fn set_field(&mut self, i: usize, f: Field) {
        self.field_vals[i] = f;
    }

    /// Returns an iterator over the field values.
    pub fn field_vals(&self) -> impl Iterator<Item = &Field> {
        self.field_vals.iter()
    }

    /// Return the length of the tuple.
    pub fn size(&self) -> usize {
        self.field_vals.len()
    }

    /// Append another tuple with self.
    ///
    /// # Arguments
    ///
    /// * `other` - Other tuple to append.
    pub fn merge(&self, other: &Self) -> Self {
        let mut fields = self.field_vals.clone();
        fields.append(&mut other.field_vals.clone());
        Self::new(fields)
    }

    pub fn get_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        serde_cbor::from_slice(bytes).unwrap()
    }

    pub fn to_csv(&self) -> String {
        let mut res = Vec::new();
        for field in &self.field_vals {
            let val = match field {
                Field::IntField(i) => i.to_string(),
                Field::StringField(s) => s.to_string(),
            };
            res.push(val);
        }
        res.join(",")
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut res = String::new();
        for field in &self.field_vals {
            let val = match field {
                Field::IntField(i) => i.to_string(),
                Field::StringField(s) => s.to_string(),
            };
            res.push_str(&val);
            res.push('\t');
        }
        write!(f, "{}", res)
    }
}

#[derive(Clone)]
// If deltas (changes) are allowed, then how
pub enum DeltaType {
    None,
    SimpleDelta,
}

#[derive(Clone)]
/// How to make a container snapshot
pub enum SnapshotType {
    SimpleSnapshot,
}

/// If we support merging of different records, how to select the right one
pub enum MergeOptions {
    /// Merging is not allowed
    None,
    /// Always use the last version of a record
    TakeLast,
    /// Custom merging takes in an initial value and a merging function.
    CustomMerging(
        ValueDiff,
        Box<dyn Fn(ValueDiff, &ValueDiff) -> ValueDiff + Send + Sync>,
    ),
}

#[derive(Clone)]
pub enum DeltaCollectionType {
    None,
    SimpleDeltaCollection,
}

/// The configuration options for a container.
/// * `merge_options` - If merges are allowed, how should they work
/// * `snapshot_type` - Snapshot container type for container.
/// * `delta_type` - If the container supports deltas, which type
pub struct ContainerConfig {
    pub merge_options: MergeOptions,
    pub snapshot_type: SnapshotType,
    pub delta_type: DeltaType,
    pub delta_container_type: DeltaCollectionType,
}

impl ContainerConfig {
    pub fn simple_delta() -> Self {
        ContainerConfig {
            merge_options: MergeOptions::TakeLast,
            snapshot_type: SnapshotType::SimpleSnapshot,
            delta_type: DeltaType::SimpleDelta,
            delta_container_type: DeltaCollectionType::SimpleDeltaCollection,
        }
    }
    pub fn simple_container() -> Self {
        ContainerConfig {
            merge_options: MergeOptions::None,
            snapshot_type: SnapshotType::SimpleSnapshot,
            delta_type: DeltaType::None,
            delta_container_type: DeltaCollectionType::None,
        }
    }
}

/// Retrieve the name from the command parser object.
///
/// # Argument
///
/// * `name` - Name object from the command parser.
pub fn get_name(name: &ast::ObjectName) -> Result<String, CrustyError> {
    if name.0.len() > 1 {
        Err(CrustyError::CrustyError(String::from(
            "Error no . names supported",
        )))
    } else {
        Ok(name.0[0].value.clone())
    }
}

/// Retrieve the dtype from the command parser object.
///
/// # Argument
///
/// * `dtype` - Name object from the command parser.
pub fn get_attr(dtype: &ast::DataType) -> Result<DataType, CrustyError> {
    match dtype {
        ast::DataType::Int => Ok(DataType::Int),
        ast::DataType::Varchar(_) => Ok(DataType::String),
        //TODO append type
        _ => Err(CrustyError::CrustyError(String::from(
            "Unsupported data type ",
        ))),
    }
}

pub enum QueryResultType {
    CSV(bool), // header
    WIDTH(bool, usize), // header, default width
               //BINARY,
}

#[cfg(test)]
mod libtests {
    use super::*;
    use crate::testutil::*;

    #[test]
    fn test_tuple_bytes() {
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple_bytes = tuple.get_bytes();
        let check_tuple: Tuple = Tuple::from_bytes(&tuple_bytes);
        assert_eq!(tuple, check_tuple);
    }
}
