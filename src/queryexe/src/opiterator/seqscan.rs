use super::OpIterator;
use crate::StorageManager;
use common::ids::Permissions;
use common::ids::{ContainerId, TransactionId};
use common::storage_trait::StorageTrait;
use common::table::*;
use common::{Attribute, CrustyError, TableSchema, Tuple};
use std::sync::{Arc, RwLock};

/// Sequential scan operator
pub struct SeqScan {
    file_iter: <StorageManager as StorageTrait>::ValIterator,
    schema: TableSchema,
    open: bool,
    storage_manager: &'static StorageManager,
    container_id: ContainerId,
    transaction_id: TransactionId,
}

impl SeqScan {
    /// Constructor for the sequential scan operator.
    ///
    /// # Arguments
    ///
    /// * `table` - Table to scan over.
    /// * `table_alias` - Table alias given by the user.
    /// * `tid` - Transaction used to read the table.
    pub fn new(
        storage_manager: &'static StorageManager,
        table: Arc<RwLock<Table>>,
        table_alias: &str,
        container_id: &ContainerId,
        tid: TransactionId,
    ) -> Self {
        let table_ref = table.read().unwrap();
        let schema = table_ref.schema.clone();
        let file_iter = storage_manager.get_iterator(*container_id, tid, Permissions::ReadOnly);
        Self {
            file_iter,
            schema: Self::schema(&schema, table_alias),
            open: false,
            storage_manager,
            container_id: *container_id,
            transaction_id: tid,
        }
    }

    /// Returns the schema of the table with aliases.
    ///
    /// # Arguments
    /// * `src_schema` - Schema of the source.
    /// * `alias` - Alias of the table.
    fn schema(src_schema: &TableSchema, alias: &str) -> TableSchema {
        let mut attrs = Vec::new();
        for a in src_schema.attributes() {
            let new_name = format!("{}.{}", alias, a.name());
            attrs.push(Attribute::new_with_constraint(
                new_name,
                a.dtype().clone(),
                a.constraint.clone(),
            ));
        }
        TableSchema::new(attrs)
    }
}

impl OpIterator for SeqScan {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        match self.file_iter.next() {
            Some(bytes) => Ok(Some(Tuple::from_bytes(&bytes))),
            None => Ok(None),
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.file_iter = self.storage_manager.get_iterator(
            self.container_id,
            self.transaction_id,
            Permissions::ReadOnly,
        );
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::opiterator::testutil::sum_int_fields;
    use common::ids::TransactionId;
    use common::testutil::get_int_table_schema;

    use common::testutil::*;

    const CHECKSUM: i32 = 18;
    const WIDTH: usize = 3;
    const TABLE: &str = "SeqScan";

    fn get_scan() -> Result<SeqScan, CrustyError> {
        // Create test table
        let schema = get_int_table_schema(WIDTH);
        let table = Arc::new(RwLock::new(Table::new(TABLE.to_string(), schema)));
        // Create test SM with a container
        let smb = Box::new(StorageManager::new_test_sm());
        let sm: &'static StorageManager = Box::leak(smb);
        let cid = 0;
        sm.create_table(cid).unwrap();
        // Create test data
        let tuple = int_vec_to_tuple(vec![1, 2, 3]);
        let tuple2 = int_vec_to_tuple(vec![1, 2, 3]);
        let tuple3 = int_vec_to_tuple(vec![1, 2, 3]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();

        let tid = TransactionId::new();
        let _rid = sm.insert_value(cid, tuple_bytes, tid);
        let _rid2 = sm.insert_value(cid, tuple_bytes2, tid);
        let _rid3 = sm.insert_value(cid, tuple_bytes3, tid);

        Ok(SeqScan::new(sm, table, TABLE, &cid, tid))
    }
    
    #[test]
    fn test_open() -> Result<(), CrustyError> {
        let mut scan = get_scan()?;
        assert!(!scan.open);
        scan.open()?;
        assert!(scan.open);
        Ok(())
    }

    #[test]
    fn test_next() -> Result<(), CrustyError> {
        let mut scan = get_scan()?;
        scan.open()?;
        assert_eq!(sum_int_fields(&mut scan)?, CHECKSUM);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_next_not_open() {
        let mut scan = get_scan().unwrap();
        scan.next();
    }

    #[test]
    fn test_close() -> Result<(), CrustyError> {
        let mut scan = get_scan()?;
        scan.open()?;
        assert!(scan.open);
        scan.close()?;
        assert!(!scan.open);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_rewind_not_open() {
        let mut scan = get_scan().unwrap();
        scan.rewind();
    }

    #[test]
    fn test_rewind() -> Result<(), CrustyError> {
        let mut scan = get_scan()?;
        scan.open()?;
        let sum_before = sum_int_fields(&mut scan)?;
        scan.rewind()?;
        let sum_after = sum_int_fields(&mut scan)?;
        assert_eq!(sum_before, sum_after);
        Ok(())
    }

    #[test]
    fn test_get_schema() {
        let scan = get_scan().unwrap();
        let original = get_int_table_schema(WIDTH);
        let prefixed = scan.get_schema();
        assert_eq!(original.size(), scan.get_schema().size());
        for (orig_attr, prefixed_attr) in original.attributes().zip(prefixed.attributes()) {
            assert_eq!(
                format!("{}.{}", TABLE, orig_attr.name()),
                prefixed_attr.name()
            );
        }
    }
}
