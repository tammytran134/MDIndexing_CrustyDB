use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::Ordering;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::i32::{MAX, MIN};
use std::sync::{Arc, RwLock};

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone, Copy, Hash)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    group_map: HashMap<Tuple, Arc<RwLock<Vec<(Field, i32)>>>>,
    agg_map: HashMap<usize, (Field, i32)>,
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        Self {
            agg_fields,
            groupby_fields,
            schema: schema.clone(),
            group_map: HashMap::new(),
            agg_map: HashMap::new(),
        }
    }

    fn initialize_agg_field(agg_field: &AggregateField, is_int_field: bool) -> (Field, i32) {
        match agg_field.op {
            AggOp::Avg => (Field::IntField(0), 0),
            AggOp::Count => (Field::IntField(0), 0),
            AggOp::Max => {
                if is_int_field {
                    (Field::IntField(MIN), 0)
                } else {
                    (Field::StringField("A".to_string()), 0)
                }
            }
            AggOp::Min => {
                if is_int_field {
                    (Field::IntField(MAX), 0)
                } else {
                    (Field::StringField("Z".to_string()), 0)
                }
            }
            AggOp::Sum => (Field::IntField(0), 0),
        }
    }

    fn get_bigger_string(s1: String, s2: String) -> String {
        match s1.cmp(&s2) {
            Ordering::Less => s2,
            _ => s1,
        }
    }

    fn get_smaller_string(s1: String, s2: String) -> String {
        match s1.cmp(&s2) {
            Ordering::Less => s1,
            _ => s2,
        }
    }

    fn modify_agg_field(
        agg_field: &AggregateField,
        curr_arg0: &common::Field,
        curr_arg1: &i32,
        field_val: &Field,
        is_int_field: bool,
    ) -> (Field, i32) {
        match agg_field.op {
            AggOp::Avg => (
                Field::IntField(curr_arg0.unwrap_int_field() + field_val.unwrap_int_field()),
                (curr_arg1 + 1),
            ),
            AggOp::Count => (Field::IntField(curr_arg0.unwrap_int_field() + 1), 0),
            AggOp::Max => {
                if is_int_field {
                    (
                        Field::IntField(max(
                            curr_arg0.unwrap_int_field(),
                            field_val.unwrap_int_field(),
                        )),
                        0,
                    )
                } else {
                    (
                        Field::StringField(Aggregator::get_bigger_string(
                            curr_arg0.unwrap_string_field().to_string(),
                            field_val.unwrap_string_field().to_string(),
                        )),
                        0,
                    )
                }
            }
            AggOp::Min => {
                if is_int_field {
                    (
                        Field::IntField(min(
                            curr_arg0.unwrap_int_field(),
                            field_val.unwrap_int_field(),
                        )),
                        0,
                    )
                } else {
                    (
                        Field::StringField(Aggregator::get_smaller_string(
                            curr_arg0.unwrap_string_field().to_string(),
                            field_val.unwrap_string_field().to_string(),
                        )),
                        0,
                    )
                }
            }
            AggOp::Sum => (
                Field::IntField(curr_arg0.unwrap_int_field() + field_val.unwrap_int_field()),
                0,
            ),
        }
    }

    fn add_agg_to_agg_map(
        &mut self,
        agg_field: &AggregateField,
        is_int_field: bool,
        agg_indice: usize,
    ) {
        self.agg_map.insert(
            agg_indice,
            Aggregator::initialize_agg_field(agg_field, is_int_field),
        );
    }

    fn add_val_to_agg_map(
        &mut self,
        field_val: &Field,
        agg_field: &AggregateField,
        is_int_field: bool,
        agg_indice: usize,
    ) {
        if !self.agg_map.contains_key(&agg_indice) {
            self.add_agg_to_agg_map(agg_field, is_int_field, agg_indice);
        }
        let (curr_arg0, curr_arg1) = self.agg_map.get(&agg_indice).unwrap();
        let new_agg_res =
            Aggregator::modify_agg_field(agg_field, curr_arg0, curr_arg1, field_val, is_int_field);
        *self.agg_map.get_mut(&agg_indice).unwrap() = new_agg_res;
    }

    fn composite_groupby_fields_from_tuple(&self, tuple: &Tuple) -> Tuple {
        let mut res = Vec::new();
        for single_groupby_field in self.groupby_fields.iter() {
            let field_value = tuple.get_field(*single_groupby_field).unwrap();
            res.push(field_value.copy_field());
        }
        Tuple::new(res)
    }

    fn add_group_to_group_map(&mut self, composite_groupby_fields: Tuple) {
        let mut agg_fields = Vec::new();
        let i = self.groupby_fields.len();
        for (j, agg_field) in self.agg_fields.iter().enumerate() {
            let attribute = self.schema.get_attribute(i + j).unwrap();
            let is_int_field;
            match attribute.dtype() {
                DataType::Int => is_int_field = true,
                _ => is_int_field = false,
            }
            agg_fields.push(Aggregator::initialize_agg_field(agg_field, is_int_field));
        }
        self.group_map
            .insert(composite_groupby_fields, Arc::new(RwLock::new(agg_fields)));
    }

    fn add_val_to_group_map(&mut self, composite_groupby_fields: Tuple, tuple: &Tuple) {
        if !self.group_map.contains_key(&composite_groupby_fields) {
            self.add_group_to_group_map(composite_groupby_fields.clone());
        }
        for (i, agg_field) in self.agg_fields.iter().enumerate() {
            let (curr_arg0, curr_arg1) = self
                .group_map
                .get(&composite_groupby_fields)
                .unwrap()
                .read()
                .unwrap()
                .get(i)
                .unwrap()
                .clone();
            let field_val = tuple.get_field(agg_field.field).unwrap();
            let new_agg_res = Aggregator::modify_agg_field(
                agg_field,
                &curr_arg0,
                &curr_arg1,
                field_val,
                field_val.is_int_field(),
            );
            self.group_map
                .get(&composite_groupby_fields)
                .unwrap()
                .write()
                .unwrap()[i] = new_agg_res;
        }
    }

    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        if self.groupby_fields.is_empty() {
            for (i, single_agg_field) in self.agg_fields.clone().iter().enumerate() {
                let field_value = tuple.get_field(single_agg_field.field).unwrap();
                self.add_val_to_agg_map(
                    field_value,
                    single_agg_field,
                    field_value.is_int_field(),
                    i,
                );
            }
        } else {
            let composite_groupby_fields = self.composite_groupby_fields_from_tuple(tuple);
            self.add_val_to_group_map(composite_groupby_fields, tuple);
        }
    }

    fn get_final_agg_res(field: &Field, info: i32, agg_field: &AggregateField) -> Field {
        match agg_field.op {
            AggOp::Avg => {
                if info == 0 {
                    Field::IntField(0)
                } else {
                    Field::IntField(field.unwrap_int_field() / info)
                }
            }
            _ => field.copy_field(),
        }
    }

    fn get_agg_tuple(&self, field_vec: &Vec<(Field, i32)>) -> Tuple {
        let mut res = Vec::new();
        for (i, (field, info)) in field_vec.iter().enumerate() {
            res.push(Aggregator::get_final_agg_res(
                field,
                *info,
                &self.agg_fields[i],
            ));
        }
        Tuple::new(res)
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        if self.groupby_fields.is_empty() {
            let mut res_field = Vec::new();
            for (i, agg_field) in self.agg_fields.clone().iter().enumerate() {
                let (field, info) = self.agg_map.get(&i).unwrap();
                res_field.push(Aggregator::get_final_agg_res(field, *info, agg_field));
            }
            TupleIterator::new(vec![Tuple::new(res_field)], self.schema.clone())
        } else {
            let mut res_tuples = Vec::new();
            for (key, val) in self.group_map.iter() {
                let agg_fields = self.get_agg_tuple(&val.read().unwrap());
                res_tuples.push(Tuple::merge(key, &agg_fields));
            }
            TupleIterator::new(res_tuples, self.schema.clone())
        }
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    gfroupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,

    aggregator: Aggregator,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        let schema = Aggregate::get_schema_from_child(
            &groupby_indices,
            &groupby_names,
            &agg_indices,
            &agg_names,
            &ops,
            &child,
        );
        let mut agg_fields = Vec::new();
        for (i, agg_indice) in agg_indices.iter().enumerate() {
            agg_fields.push(AggregateField {
                field: *agg_indice,
                op: ops[i],
            });
        }
        Self {
            gfroupby_fields: groupby_indices.clone(),
            agg_fields: agg_fields.clone(),
            agg_iter: None,
            schema: schema.clone(),
            open: false,
            child,
            aggregator: Aggregator::new(agg_fields, groupby_indices, &schema),
        }
    }

    fn get_agg_attribute_type(child_attribute: Attribute, op: AggOp) -> DataType {
        match op {
            AggOp::Avg => DataType::Int,
            AggOp::Count => DataType::Int,
            AggOp::Max => child_attribute.dtype,
            AggOp::Min => child_attribute.dtype,
            AggOp::Sum => DataType::Int,
        }
    }

    fn get_schema_from_child(
        groupby_indices: &Vec<usize>,
        groupby_names: &Vec<&str>,
        agg_indices: &Vec<usize>,
        agg_names: &Vec<&str>,
        ops: &Vec<AggOp>,
        child: &Box<dyn OpIterator>,
    ) -> TableSchema {
        let mut parent_schema = Vec::new();
        let child_schema = child.get_schema();
        for (i, groupby_indice) in groupby_indices.iter().enumerate() {
            let child_attribute = child_schema.get_attribute(*groupby_indice).unwrap();
            parent_schema.push(Attribute::new(
                groupby_names[i].to_string(),
                child_attribute.clone().dtype,
            ));
        }
        for (i, agg_indice) in agg_indices.iter().enumerate() {
            let child_attribute = child_schema.get_attribute(*agg_indice).unwrap();
            parent_schema.push(Attribute::new(
                agg_names[i].to_string(),
                Aggregate::get_agg_attribute_type(child_attribute.clone(), ops[i]),
            ));
        }
        TableSchema::new(parent_schema)
    }
}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.child.open()?;
        while let Some(input_tuple) = self.child.next()? {
            self.aggregator.merge_tuple_into_group(&input_tuple);
        }
        self.agg_iter = Some(self.aggregator.iterator());
        self.agg_iter.as_mut().unwrap().open()?;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        self.agg_iter.as_mut().unwrap().next()
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.child.close()?;
        self.agg_iter.as_mut().unwrap().close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.child.rewind()?;
        self.agg_iter.as_mut().unwrap().rewind()?;
        self.close()?;
        self.open()?;
        self.agg_iter.as_mut().unwrap().open()?;
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
