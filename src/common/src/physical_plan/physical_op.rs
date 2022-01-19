use crate::logical_plan::{FieldIdentifier, Predicate, ProjectIdentifiers, SimplePredicateOp};
use crate::prelude::*;

/// Physical Scan Operator
/// Same as Logical
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalScanNode {
    pub alias: String,
    pub container_id: ContainerId,
}

/// Physical Project Operator
/// Same as Logical
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalProjectNode {
    pub identifiers: ProjectIdentifiers,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalHashAggregateNode {
    /// Fields to aggregate.
    pub fields: Vec<FieldIdentifier>,
    /// Fields to groupby.
    pub group_by: Vec<FieldIdentifier>,
    /// ID of the Hash Table to use. only needed if saving the hash table
    pub hash_table_state_id: ContainerId,
    /// Vector of the keys to hash by
    pub hash_table_key: Vec<FieldIdentifier>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalSortedAggregateNode {
    /// Fields to aggregate.
    pub fields: Vec<FieldIdentifier>,
    /// Fields to groupby.
    pub group_by: Vec<FieldIdentifier>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalNestedLoopJoinNode {
    /// Left side of the operator.
    pub left: FieldIdentifier,
    /// Right side of the operator.
    pub right: FieldIdentifier,
    /// Predicate operator.
    pub op: SimplePredicateOp,
    /// Right table.
    pub left_table: Option<String>,
    /// Left table.
    pub right_table: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalHashJoinNode {
    /// Left side of the operator.
    pub left: FieldIdentifier,
    /// Right side of the operator.
    pub right: FieldIdentifier,
    /// Predicate operator.
    pub op: SimplePredicateOp,
    /// Right table.
    pub left_table: Option<String>,
    /// Left table.
    pub right_table: Option<String>,
    /// ID of the Hash Table to use. only needed if saving the hash table
    pub hash_table_state_id: ContainerId,
    /// Vector of the keys to hash by (seems like it may only need to be a single key for now)
    pub hash_table_key: FieldIdentifier,
}

/// Physical Filter Operator
/// Same as Logical for now, but may want to add extra information
/// Like what order to perform the checks in a composite filter
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalFilterNode {
    /// Table to filter.
    pub table: String,
    /// Predicate to filter by.
    pub predicate: Predicate,
}

/// Materialized View Node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MaterializedViewNode {
    /// the ID of the materialized view
    pub materialized_view_state_id: ContainerId,
}
