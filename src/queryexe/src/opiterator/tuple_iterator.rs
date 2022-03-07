use super::OpIterator;
use common::{CrustyError, TableSchema, Tuple};

/// Iterator over a Vec of tuples, mainly used for testing.
pub struct TupleIterator {
    /// Tuples to iterate over.
    tuples: Vec<Tuple>,
    /// Schema of the output.
    schema: TableSchema,
    /// Current tuple in iteration.
    index: Option<usize>,
}

impl TupleIterator {
    /// Create a new tuple iterator over a set of results.
    ///
    /// # Arguments
    ///
    /// * `tuples` - Tuples to iterate over.
    /// * `schema` - Schema of the output results.
    pub fn new(tuples: Vec<Tuple>, schema: TableSchema) -> Self {
        Self {
            index: None,
            tuples,
            schema,
        }
    }
}

impl OpIterator for TupleIterator {
    /// Opens the iterator without returning a tuple.
    fn open(&mut self) -> Result<(), CrustyError> {
        self.index = Some(0);
        Ok(())
    }

    /// Retrieves the next tuple in the iterator.
    ///
    /// # Panics
    ///
    /// Panics if the TupleIterator has not been opened.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        let i = match self.index {
            None => panic!("Operator has not been opened"),
            Some(i) => i,
        };
        let tuple = self.tuples.get(i);
        self.index = Some(i + 1);
        Ok(tuple.cloned())
    }

    /// Closes the tuple iterator.
    fn close(&mut self) -> Result<(), CrustyError> {
        self.index = None;
        Ok(())
    }

    /// Make iterator point to the first tuple again.
    ///
    /// # Panics
    ///
    /// Panics if the TupleIterator has not been opened.
    fn rewind(&mut self) -> Result<(), CrustyError> {
        if self.index.is_none() {
            panic!("Operator has not been opened")
        }
        self.close()?;
        self.open()
    }

    /// Returns the schema of the tuples.
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    // use crate::opiterator::testutil::*;
    use common::testutil::*;

    const NUM_ROWS: usize = 3;
    const WIDTH: usize = 1;

    fn get_tuple_iterator() -> TupleIterator {
        let tuples = create_tuple_list(vec![vec![1], vec![2], vec![3]]);
        let schema = get_int_table_schema(WIDTH);
        TupleIterator::new(tuples, schema)
    }

    #[test]
    fn test_open() -> Result<(), CrustyError> {
        let mut ti = get_tuple_iterator();
        ti.open()?;
        assert!(ti.index.is_some());
        Ok(())
    }

    #[test]
    fn test_next() -> Result<(), CrustyError> {
        let mut ti = get_tuple_iterator();
        ti.open()?;
        let mut counter = 0;
        while ti.next()?.is_some() {
            counter += 1;
        }
        assert_eq!(counter, NUM_ROWS);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_next_not_open() {
        let mut ti = get_tuple_iterator();
        ti.next().unwrap();
    }

    #[test]
    fn test_close() -> Result<(), CrustyError> {
        let mut ti = get_tuple_iterator();
        ti.open()?;
        assert!(ti.index.is_some());
        ti.close()?;
        assert!(ti.index.is_none());
        Ok(())
    }

    #[test]
    fn test_rewind() -> Result<(), CrustyError> {
        let mut ti = get_tuple_iterator();
        ti.open()?;
        let mut counter1 = 0;
        while ti.next()?.is_some() {
            counter1 += 1;
        }
        ti.rewind()?;
        let mut counter2 = 0;
        while ti.next()?.is_some() {
            counter2 += 1;
        }
        assert_eq!(counter1, counter2);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_rewind_not_open() {
        let mut ti = get_tuple_iterator();
        ti.rewind().unwrap();
    }
}
