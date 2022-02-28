use super::{OpIterator, TupleIterator};
use common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple};
use std::collections::HashMap;

/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
}

impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
        Self {
            op,
            left_index,
            right_index,
        }
    }
    fn join(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> bool {
        let left_field = left_tuple.get_field(self.left_index).unwrap();
        let right_field = right_tuple.get_field(self.right_index).unwrap();
        self.op.compare(left_field, right_field)
    }
}

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator>,
    /// Right child node.
    right_child: Box<dyn OpIterator>,
    /// Schema of the result.
    schema: TableSchema,
    /// Boolean determining if left iterator is open
    left_open: bool,
    /// Boolean determining if right iterator is open
    right_open: bool,
    curr_left: Option<Tuple>,
    curr_right: Option<Tuple>,
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        let left_schema = left_child.get_schema().clone();
        let right_schema = right_child.get_schema().clone();
        Self {
            predicate: JoinPredicate {
                op,
                left_index,
                right_index,
            },
            left_child,
            right_child,
            schema: TableSchema::merge(&left_schema, &right_schema),
            left_open: false,
            right_open: false,
            curr_left: None,
            curr_right: None,
        }
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.left_open = true;
        self.left_child.open()?;
        self.right_open = true;
        self.right_child.open()?;
        Ok(())
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.left_open || !self.right_open {
            panic!("Operator has not been opened")
        }
        let mut res = None;
        if self.curr_right.is_none() {
            self.right_child.rewind()?;
            self.curr_right = self.right_child.next()?;
            self.curr_left = self.left_child.next()?;
            if self.curr_left.is_none() {
                return Ok(res);
            }
        }
        while self.curr_right.is_some() && self.curr_left.is_some() {
            let left_tuple = self.curr_left.as_ref().unwrap();
            let right_tuple = &self.curr_right.as_ref().unwrap();
            if self.predicate.join(left_tuple, right_tuple) {
                res = Some(Tuple::merge(left_tuple, right_tuple));
                self.curr_right = self.right_child.next()?;
                return Ok(res);
            }
            self.curr_right = self.right_child.next()?;
        }
        self.next()
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.left_open || !self.right_open {
            panic!("Operator has not been opened")
        }
        self.left_child.close()?;
        self.right_child.close()?;
        self.left_open = false;
        self.right_open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.left_open || !self.right_open {
            panic!("Operator has not been opened")
        }
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.close()?;
        self.curr_right = None;
        self.curr_left = None;
        self.open()
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    predicate: JoinPredicate,

    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    curr_left: Option<Tuple>,
    schema: TableSchema,
    left_open: bool,
    right_open: bool,
    join_map: HashMap<Field, Vec<Tuple>>,
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        let left_schema = left_child.get_schema().clone();
        let right_schema = right_child.get_schema().clone();
        Self {
            predicate: JoinPredicate {
                op,
                left_index,
                right_index,
            },
            left_child,
            right_child,
            curr_left: None,
            schema: TableSchema::merge(&left_schema, &right_schema),
            left_open: false,
            right_open: false,
            join_map: HashMap::new(),
        }
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.left_open = true;
        self.left_child.open()?;
        self.right_open = true;
        self.right_child.open()?;
        while let Some(right) = self.right_child.next()? {
            let right_field = right.get_field(self.predicate.right_index).unwrap();
            self.join_map
                .entry(right_field.clone())
                .or_default()
                .push(right);
        }
        self.curr_left = self.left_child.next()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.left_open || !self.right_open {
            panic!("Operator has not been opened")
        }
        let mut res = None;
        if self.curr_left.is_none() {
            return Ok(res);
        }
        while self.curr_left.is_some() {
            let left = self.curr_left.as_ref().unwrap();
            let left_field = left.get_field(self.predicate.left_index).unwrap();
            match self.join_map.get_mut(left_field) {
                None => {
                    self.curr_left = self.left_child.next()?;
                    return self.next();
                }
                Some(right_matches) => {
                    let first_right_match = right_matches.pop();
                    if first_right_match == None {
                        self.join_map.remove(left_field);
                        return self.next();
                    } else {
                        res = Some(Tuple::merge(left, &first_right_match.unwrap()));
                        return Ok(res);
                    }
                }
            }
        }
        Ok(res)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.left_open || !self.right_open {
            panic!("Operator has not been opened")
        }
        self.left_child.close()?;
        self.right_child.close()?;
        self.left_open = false;
        self.right_open = false;
        self.join_map = HashMap::new();
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.left_open || !self.right_open {
            panic!("Operator has not been opened")
        }
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.close()?;
        self.open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;
    use common::testutil::*;

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5, 6],
            vec![5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3],
            vec![3, 4, 3, 4, 5],
            vec![5, 6, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn gt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![3, 4, 1, 2, 3], // 1, 2 < 3
            vec![3, 4, 2, 3, 4],
            vec![5, 6, 1, 2, 3], // 1, 2, 3, 4 < 5
            vec![5, 6, 2, 3, 4],
            vec![5, 6, 3, 4, 5],
            vec![5, 6, 4, 5, 6],
            vec![7, 8, 1, 2, 3], // 1, 2, 3, 4, 5 < 7
            vec![7, 8, 2, 3, 4],
            vec![7, 8, 3, 4, 5],
            vec![7, 8, 4, 5, 6],
            vec![7, 8, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 2, 3, 4], // 1 < 2, 3, 4, 5
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 4, 5, 6], // 3 < 4, 5
            vec![3, 4, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_or_eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3], // 1 <= 1, 2, 3, 4, 5
            vec![1, 2, 2, 3, 4],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 3, 4, 5], // 3 <= 3, 4, 5
            vec![3, 4, 4, 5, 6],
            vec![3, 4, 5, 6, 7],
            vec![5, 6, 5, 6, 7], // 5 <= 5
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(op, left_index, right_index, s1, s2)),
            JoinType::HashEq => Box::new(HashEqJoin::new(op, left_index, right_index, s1, s2)),
        }
    }

    fn test_get_schema(join_type: JoinType) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.next().unwrap();
    }

    fn test_close_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.close().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;
        let mut eq_join = eq_join();
        eq_join.open()?;
        let acutal = op.next()?;
        let expected = eq_join.next()?;
        assert_eq!(acutal, expected);
        Ok(())
    }

    fn test_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let mut eq_join = eq_join();
        op.open()?;
        eq_join.open()?;
        match_all_tuples(op, Box::new(eq_join))
    }

    fn test_gt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::GreaterThan, 0, 0);
        let mut gt_join = gt_join();
        op.open()?;
        gt_join.open()?;
        match_all_tuples(op, Box::new(gt_join))
    }

    fn test_lt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThan, 0, 0);
        let mut lt_join = lt_join();
        op.open()?;
        lt_join.open()?;
        match_all_tuples(op, Box::new(lt_join))
    }

    fn test_lt_or_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThanOrEq, 0, 0);
        let mut lt_or_eq_join = lt_or_eq_join();
        op.open()?;
        lt_or_eq_join.open()?;
        match_all_tuples(op, Box::new(lt_or_eq_join))
    }

    mod join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn close_not_open() {
            test_close_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::NestedLoop);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::NestedLoop)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::NestedLoop)
        }

        #[test]
        fn gt_join() -> Result<(), CrustyError> {
            test_gt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_join() -> Result<(), CrustyError> {
            test_lt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_or_eq_join() -> Result<(), CrustyError> {
            test_lt_or_eq_join(JoinType::NestedLoop)
        }
    }

    mod hash_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::HashEq);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::HashEq)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::HashEq)
        }
    }
}
