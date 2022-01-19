use std::fmt;

use crate::ids::{StateMeta, StateType};
use crate::prelude::*;
use crate::ContainerConfig;

// TODO: Add serialization support via typetags and regular serde.
// TODO: Add iterator support for ContainerDelta and ContainerBase.
// TODO: Generalize container_id to be able to generate them automatically.

/// Trait for a delta storage manager. It supports persisting intermediate state, metadata, and
/// base tables. Thus, the storage managers will have to interact with the rust containers directly as we do not want to pay
/// serialization costs for intermediate state when on the same node. It will also work with deltas on top of the base
/// container.
///
/// The StorageManager workload is handle containers that have a single-writer and multiple
/// readers. Thus, we can have lock-free data structures as long as we also ensure that ContainerIDs
/// are generated deterministically.
pub trait DeltaStorageManagerTrait {
    /// Create a new storage manager.
    fn new() -> Self;

    /// Checks if the storage manager has a container with the given id
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to check if exists
    fn has_container(&self, container_id: &ContainerId) -> bool;

    /// Creates a new container object.
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        delta_container_config: ContainerConfig,
        name: Option<String>,
        container_type: StateType,
        dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError>;

    fn get_simple_config() -> ContainerConfig;

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        tid: TransactionId,
        container_id: ContainerId,
        timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError>;

    /// Remove a container.
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of the container to remove.
    fn delete_container(&self, container_id: &ContainerId);

    /// Add a delta to container. Must pass in the publisher's executor id.
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    /// * `timestamp` - Timestamp of the container delta.
    /// * `keys` - Keys for the new delta.
    /// * `values` - Values for the new delta.
    fn add_delta(
        &self,
        container_id: &ContainerId,
        timestamp: LogicalTimeStamp,
        keys: Vec<Key>,
        values: Vec<ValueDiff>,
    ) -> Result<(), CrustyError>;

    /// Get keys from container up to some timestamp.
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to read.
    /// * `timestamp` - Timestamp to read upto.
    fn keys_upto_timestamp(
        &self,
        container_id: &ContainerId,
        timestamp: LogicalTimeStamp,
    ) -> Option<Vec<Key>>;

    /// Get keys from container up to some timestamp.
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to read.
    /// * `timestamp` - Timestamp to read upto.
    fn keys_timestamp_range(
        &self,
        container_id: &ContainerId,
        start_timestamp: LogicalTimeStamp,
        end_timestamp: LogicalTimeStamp,
    ) -> Option<Vec<Key>>;

    /// Read container value compacted up to some timestamp.
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to read.
    /// * `key` - Key to read.
    /// * `timestamp` - Timestamp to read upto.
    fn read(
        &self,
        container_id: &ContainerId,
        key: &Key,
        timestamp: LogicalTimeStamp,
    ) -> Option<ValueDiff>;

    /// Read deltas of an container by returning an in-memory copy of the delta fragments (deltas between
    /// left-inclusive timestamps).
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to read.
    /// * `key` - Key to read.
    /// * `start_ts` - Timestamp to start reading deltas of.
    /// * `end_ts` - Timestamp to end reading deltas of.
    fn read_diffs(
        &self,
        container_id: &ContainerId,
        key: &Key,
        start_timestamp: LogicalTimeStamp,
        end_timestamp: LogicalTimeStamp,
    ) -> Option<Vec<ValueDiff>>;

    /// Compact container.  
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    /// * `timestamp` - LogicalTimeStamp to compact upto.
    fn compact_container(&self, container_id: &ContainerId, timestamp: LogicalTimeStamp);

    /// Get the last time the container was updated, if at all
    fn get_last_update(&self, container_id: &ContainerId) -> Option<LogicalTimeStamp>;

    /// Get the containers not above the watermark (whose last update is lower than watermark AND could be updated)
    /// Return empty vec if all are updated or there are no pending updates.
    fn get_outdated_containers(
        &self,
        containers: &[ContainerId],
        watermark: LogicalTimeStamp,
    ) -> Vec<ContainerId>;

    fn get_state_meta(&self, container_id: &ContainerId) -> Option<StateMeta>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueDiff {
    Upsert(Value),
    Delete,
}

impl fmt::Display for ValueDiff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueDiff::Upsert(val) => write!(f, "Upsert({})", val),
            ValueDiff::Delete => write!(f, "Delete"),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ValueId {
    key: Value,
    timestamp: LogicalTimeStamp,
}

impl ValueId {
    pub fn get_key(&self) -> &Value {
        &self.key
    }

    pub fn get_timestamp(&self) -> LogicalTimeStamp {
        self.timestamp
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Value {
    ByteArray(Vec<u8>),
    StringValue(String),
    IntegerValue(u32),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::ByteArray(val) => write!(f, "ByteArray({:?})", val),
            Value::StringValue(val) => write!(f, "StringValue({:?})", val),
            Value::IntegerValue(val) => write!(f, "IntegerValue({:?})", val),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Key {
    RawValue(Value),
    ValueId(ValueId),
}

impl Key {
    pub fn get_value(&self) -> Value {
        match self {
            Key::RawValue(value) => value.clone(),
            Key::ValueId(value_id) => value_id.get_key().clone(),
        }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Key::RawValue(value) => write!(f, "Key(RawValue({:?}))", value),
            Key::ValueId(ValueId { key, timestamp }) => {
                write!(f, "Key(ValueId({:?}, {:?}))", timestamp, key)
            }
        }
    }
}
