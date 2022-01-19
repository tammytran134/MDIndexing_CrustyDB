use super::OpIterator;
use common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple};

/// Compares the fields of tuples.
pub struct FilterPredicate {
    /// Operation used to compare.
    op: SimplePredicateOp,
    /// Index of the field to filter.
    field_ind: usize,
    /// Operand to compare against.
    operand: Field,
}

impl FilterPredicate {
    /// Constructor - Creates a new predicate over two fields of two tuples
    ///
    /// # Arguments
    ///
    /// * `op` - The operation to apply (as defined in common-old::SimplePredicateOp)
    /// * `field_ind` - Field index to compare against
    /// * `operand` - Field value to compare passed in tuples to    
    fn new(op: SimplePredicateOp, field_ind: usize, operand: Field) -> Self {
        Self {
            op,
            field_ind,
            operand,
        }
    }

    /// Apply the predicate to the specified tuple.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to apply the filter to.
    fn filter(&self, tuple: &Tuple) -> bool {
        let field = tuple.get_field(self.field_ind).unwrap();
        self.op.compare(field, &self.operand)
    }
}

/// Filter oeprator.
pub struct Filter {
    /// Predicate to filter by.
    predicate: FilterPredicate,
    /// Schema of the child.
    schema: TableSchema,
    /// Boolean determining if iterator is open.
    open: bool,
    /// Child operator passing data into operator.
    child: Box<dyn OpIterator>,
}

impl Filter {
    /// Filter constructor.
    ///
    /// # Arguments
    ///
    /// * `predicate` - Predicate to filter by.
    /// * `child` - Child OpIterator passing data into the operator.
    pub fn new(
        op: SimplePredicateOp,
        field_ind: usize,
        operand: Field,
        child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            predicate: FilterPredicate::new(op, field_ind, operand),
            schema: child.get_schema().clone(),
            open: false,
            child,
        }
    }
}

impl OpIterator for Filter {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        let mut res = None;
        while let Some(t) = self.child.next()? {
            if self.predicate.filter(&t) {
                res = Some(t);
                break;
            }
        }
        Ok(res)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.child.rewind()?;
        self.close()?;
        self.open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::opiterator::testutil::*;
    use common::testutil::*;

    const WIDTH: usize = 3;

    /// Creates a TupleIterator where each tuple has 'width' fields,
    /// and each feel within a tuple has the same value, that
    /// increases from low (inclusive) to high (exclusive)
    fn mock_ti(low: i32, high: i32, width: usize) -> TupleIterator {
        let mut rows = Vec::new();
        for i in low..high {
            let row = std::iter::repeat(i).take(width).collect();
            rows.push(row);
        }
        let tuples = create_tuple_list(rows);
        let schema = get_int_table_schema(width);
        TupleIterator::new(tuples.to_vec(), schema)
    }

    fn get_filter(field_num: usize, op: SimplePredicateOp, operand: Field) -> Filter {
        let ti = mock_ti(-5, 5, WIDTH);
        Filter::new(op, field_num, operand, Box::new(ti))
    }

    /// Returns a tuple with width fields, where each field contains the value repeat
    fn tuple_repeat_field(repeat: i32, width: usize) -> Tuple {
        let fields = std::iter::repeat(Field::IntField(repeat))
            .take(width)
            .collect();
        Tuple::new(fields)
    }

    #[test]
    fn test_open() -> Result<(), CrustyError> {
        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(0));
        assert!(!filter.open);
        filter.open()?;
        assert!(filter.open);
        Ok(())
    }

    #[test]
    fn get_schema() {
        let filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(0));
        let expected = get_int_table_schema(WIDTH);
        let actual = filter.get_schema();
        assert_eq!(expected, *actual);
    }

    #[test]
    #[should_panic]
    fn test_next_not_open() {
        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(0));
        filter.next().unwrap();
    }

    #[test]
    fn test_close() -> Result<(), CrustyError> {
        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(0));
        filter.open()?;
        assert!(filter.open);
        filter.close()?;
        assert!(!filter.open);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_rewind_not_open() {
        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(0));
        filter.rewind().unwrap();
    }

    #[test]
    fn test_rewind() -> Result<(), CrustyError> {
        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(0));
        filter.open()?;
        assert!(filter.next()?.is_some());

        filter.rewind()?;
        let expected = tuple_repeat_field(0, WIDTH);
        let actual = filter.next()?.unwrap();
        assert_eq!(expected, actual);
        filter.close()
    }

    #[test]
    fn test_some_less_than() -> Result<(), CrustyError> {
        let mut filter = get_filter(0, SimplePredicateOp::LessThan, Field::IntField(2));
        let mut expected = mock_ti(-5, 2, WIDTH);
        filter.open()?;
        expected.open()?;
        match_all_tuples(Box::new(filter), Box::new(expected))
    }

    #[test]
    fn test_all_less_than() -> Result<(), CrustyError> {
        let mut filter = get_filter(0, SimplePredicateOp::LessThan, Field::IntField(-5));
        filter.open()?;
        assert!(filter.next()?.is_none());
        Ok(())
    }

    #[test]
    fn test_equal() -> Result<(), CrustyError> {
        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(-5));
        filter.open()?;
        assert_eq!(tuple_repeat_field(-5, WIDTH), filter.next()?.unwrap());
        filter.close()?;

        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(0));
        filter.open()?;
        assert_eq!(tuple_repeat_field(0, WIDTH), filter.next()?.unwrap());
        filter.close()?;

        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(4));
        filter.open()?;
        assert_eq!(tuple_repeat_field(4, WIDTH), filter.next()?.unwrap());
        filter.close()
    }

    #[test]
    fn test_no_equal_tuples() -> Result<(), CrustyError> {
        let mut filter = get_filter(0, SimplePredicateOp::Equals, Field::IntField(5));
        filter.open()?;
        assert!(filter.next()?.is_none());
        Ok(())
    }
}
