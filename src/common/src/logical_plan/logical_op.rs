use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Debug;

use crate::ids::ContainerId;
use crate::Field;

/// Scan node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScanNode {
    /// Alias to rename when scanning.
    pub alias: String,
    pub container_id: ContainerId,
}

/// Projection node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectNode {
    /// Identifiers for which columns to keep.
    pub identifiers: ProjectIdentifiers,
}

/// Projection identifiers.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ProjectIdentifiers {
    /// All values.
    Wildcard,
    /// List of values to keep.
    List(Vec<FieldIdentifier>),
}

/// Aggregation node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggregateNode {
    /// Fields to aggregate.
    pub fields: Vec<FieldIdentifier>,
    /// Fields to groupby.
    pub group_by: Vec<FieldIdentifier>,
}

/// JoinNode
/// * left - field on left side of op
/// * op - comparison operator
/// * right - field on right side of op
/// * table1/table2 - Name of the tables being joined or none if derived table
/// table1 does not necessarily contain left, likewise with table2
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinNode {
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

/// Filter node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterNode {
    /// Table to filter.
    pub table: String,
    /// Predicate to filter by.
    pub predicate: Predicate,
}

/// Predicate to be used in filter
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Predicate {
    SimplePredicate(SimplePredicate),
    CompoundPredicate(CompoundPredicate),
}

/// All the operations that can be in a predicate
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PredicateOp {
    SimplePredicateOp(SimplePredicateOp),
    CompoundPredicateOp(CompoundPredicateOp),
}

/// Simple predicate
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SimplePredicate {
    pub left: PredExpr,
    pub op: SimplePredicateOp,
    pub right: PredExpr,
}

/// The operations which can be used in a simple predicate
impl SimplePredicateOp {
    /// Do predicate comparison.
    ///
    /// # Arguments
    ///
    /// * `left_field` - Left field of the predicate.
    /// * `right_field` - Right field of the predicate.
    pub fn compare<T: Ord>(&self, left_field: &T, right_field: &T) -> bool {
        match self {
            SimplePredicateOp::Equals => left_field == right_field,
            SimplePredicateOp::GreaterThan => left_field > right_field,
            SimplePredicateOp::LessThan => left_field < right_field,
            SimplePredicateOp::LessThanOrEq => left_field <= right_field,
            SimplePredicateOp::GreaterThanOrEq => left_field >= right_field,
            SimplePredicateOp::NotEq => left_field != right_field,
            SimplePredicateOp::All => true,
        }
    }

    /// Flip the operator.
    pub fn flip(&self) -> Self {
        match self {
            SimplePredicateOp::GreaterThan => SimplePredicateOp::LessThan,
            SimplePredicateOp::LessThan => SimplePredicateOp::GreaterThan,
            SimplePredicateOp::LessThanOrEq => SimplePredicateOp::GreaterThanOrEq,
            SimplePredicateOp::GreaterThanOrEq => SimplePredicateOp::LessThanOrEq,
            op => *op,
        }
    }
}

/// Operators for simple predicates
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum SimplePredicateOp {
    Equals,
    GreaterThan,
    LessThan,
    LessThanOrEq,
    GreaterThanOrEq,
    NotEq,
    All,
}

/// Compound Predicate
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompoundPredicate {
    pub op: CompoundPredicateOp,
    pub simple_predicates: Vec<SimplePredicate>,
}

/// Operations for compound predicates
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum CompoundPredicateOp {
    And,
    Or,
}

impl CompoundPredicateOp {
    /// Gets the identity element of this compound predicate operator
    pub fn identity(&self) -> bool {
        match self {
            CompoundPredicateOp::And => true,
            CompoundPredicateOp::Or => false,
        }
    }

    pub fn apply(&self, left: bool, right: bool) -> bool {
        match self {
            CompoundPredicateOp::And => left && right,
            CompoundPredicateOp::Or => left || right,
        }
    }
}

/// Predicate expression.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PredExpr {
    Literal(Field),
    Ident(FieldIdentifier),
}

impl PredExpr {
    /// Get the field identifier from the predicate expression.
    pub fn ident(&self) -> Option<&FieldIdentifier> {
        match self {
            PredExpr::Ident(i) => Some(i),
            _ => None,
        }
    }
}

/// Aggregation operations.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum AggOp {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl fmt::Display for AggOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op_str = match self {
            AggOp::Avg => "avg",
            AggOp::Count => "count",
            AggOp::Max => "max",
            AggOp::Min => "min",
            AggOp::Sum => "sum",
        };
        write!(f, "{}", op_str)
    }
}

/// Represents a field identifier.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FieldIdentifier {
    /// The name of table that column is present in.
    table: String,
    /// The name of the column being referenced.
    column: String,
    /// The alias given to the output field.
    alias: Option<String>,
    /// An aggregate operation performed on column.
    op: Option<AggOp>,
}

impl FieldIdentifier {
    /// Create a new field identifier.
    ///
    /// # Arguments
    ///
    /// * `table` - Table of the field.
    /// * `column` - Column.
    pub fn new(table: &str, column: &str) -> Self {
        Self {
            table: table.to_string(),
            column: column.to_string(),
            alias: None,
            op: None,
        }
    }

    /// Creates a new field identifier with alias.
    ///
    /// # Arguments
    ///
    /// * `table` - Table of the field.
    /// * `column` - Original column name.
    /// * `alias` - Column name alias.
    pub fn new_column_alias(table: &str, column: &str, alias: &str) -> Self {
        let mut id = Self::new(table, column);
        id.alias = Some(alias.to_string());
        id
    }

    /// Returns the table.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the original column name.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the field identifier alias.
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    /// Returns the aggregate operator.
    pub fn agg_op(&self) -> Option<AggOp> {
        self.op
    }

    /// Set an alias for the field identifier.
    ///
    /// # Argument
    ///
    /// * `alias` - Alias to set.
    pub fn set_alias(&mut self, alias: String) {
        self.alias = Some(alias);
    }

    /// If an op is some, sets the alias to a default alias>
    pub fn default_alias(&mut self) {
        if let Some(op) = self.op {
            self.alias = Some(format!("{}_{}", op, self.column));
        }
    }

    /// Set an aggregation operation.
    ///
    /// # Arguments
    ///
    /// * `op` - Aggregation operation to set.
    pub fn set_op(&mut self, op: AggOp) {
        self.op = Some(op);
    }
}
