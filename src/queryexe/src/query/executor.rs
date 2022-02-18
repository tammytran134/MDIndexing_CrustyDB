use crate::opiterator::*;
use crate::StorageManager;
use common::catalog::Catalog;
use common::logical_plan::*;
use common::physical_plan::*;
use common::prelude::*;
use common::{QueryResult, QueryResultType, QUERY_RESULT_TYPE};

/// Manages the execution of queries using OpIterators and converts a LogicalPlan to a tree of OpIterators and runs it.
pub struct Executor {
    /// Executor state
    pub plan: Option<Box<dyn OpIterator>>,
    pub storage_manager: &'static StorageManager,
}

impl Executor {
    /// Initializes an executor.
    ///
    /// Takes in the database catalog, query's logical plan, and transaction id to create the
    /// physical plan for the executor.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog of the database containing the metadata about the tables and such.
    /// * `storage_manager` - The SM for the DB to get access to files/buffer pool
    /// * `logical_plan` - Translated logical plan of the query.
    /// * `tid` - Id of the transaction that this executor is running.
    pub fn new_ref(storage_manager: &'static StorageManager) -> Self {
        Self {
            plan: None,
            storage_manager,
        }
    }

    pub fn configure_query(&mut self, opiterator: Box<dyn OpIterator>) {
        self.plan = Some(opiterator);
    }

    /// Returns the op plan iterator to begin execution.
    pub fn start(&mut self) -> Result<(), CrustyError> {
        self.plan.as_mut().unwrap().open()
    }

    /// Returns the next tuple or None if there is no such tuple.
    ///
    /// # Panics
    ///
    /// Panics if opiterator is closed
    // TODO(williamma12): Change Executor to have an iterator implementation.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        self.plan.as_mut().unwrap().next()
    }

    /// Closes the op iterator.
    pub fn close(&mut self) -> Result<(), CrustyError> {
        self.plan.as_mut().unwrap().close()
    }

    /// Consumes the opiterator and stores the result in a QueryResult.
    pub fn execute(&mut self) -> Result<QueryResult, CrustyError> {
        let schema = self.plan.as_mut().unwrap().get_schema();

        match QUERY_RESULT_TYPE {
            QueryResultType::WIDTH(header, default_width) => {
                let width = schema
                    .attributes()
                    .map(|a| a.name().len())
                    .max()
                    .unwrap_or(default_width)
                    + 2;
                let mut res = String::new();
                if header {
                    for attr in schema.attributes() {
                        let s = format!("{:width$}", attr.name(), width = width);
                        res.push_str(&s);
                    }
                    res.push('\n');
                }

                self.start()?;
                while let Some(t) = &self.next()? {
                    for f in t.field_vals() {
                        let s = format!("{:width$}", f.to_string(), width = width);
                        res.push_str(&s);
                    }
                    res.push('\n');
                }
                self.close()?;
                Ok(QueryResult::new(&res))
            }
            QueryResultType::CSV(header) => {
                let mut res = String::new();
                if header {
                    for attr in schema.attributes() {
                        let s = format!("{},", attr.name());
                        res.push_str(&s);
                    }
                    //remove the last ,
                    res.pop();
                    res.push('\n');
                }

                self.start()?;
                while let Some(t) = &self.next()? {
                    for f in t.field_vals() {
                        let s = format!("{},", f.to_string());
                        res.push_str(&s);
                    }
                    //remove the last ,
                    res.pop();
                    res.push('\n');
                }
                //remove the last \n
                res.pop();
                self.close()?;
                Ok(QueryResult::new(&res))
            }
        }
    }

    /// Converts a physical_plan to an op_iterator.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog of the database containing the metadata about the tables and such.
    /// * `physical_plan` - Translated physical plan of the query.
    /// * `tid` - Id of the transaction that this executor is running.
    pub fn physical_plan_to_op_iterator<T: Catalog>(
        storage_manager: &'static StorageManager,
        catalog: &T,
        physical_plan: &PhysicalPlan,
        tid: TransactionId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<Box<dyn OpIterator>, CrustyError> {
        let start = physical_plan
            .root()
            .ok_or_else(|| CrustyError::ExecutionError(String::from("No root node")))?;
        Executor::physical_plan_to_op_iterator_helper(
            storage_manager,
            catalog,
            physical_plan,
            start,
            tid,
        )
    }

    /// Recursive helper function to parse physical plan into opiterator.
    ///
    /// Function first converts all of the current nodes children to an opiterator before converting self to an opiterator.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Catalog of the database containing the metadata about the tables and such.
    /// * `physical plan` - physical plan of the query.
    /// * `tid` - Id of the transaction that this executor is running.
    fn physical_plan_to_op_iterator_helper<T: Catalog>(
        storage_manager: &'static StorageManager,
        catalog: &T,
        physical_plan: &PhysicalPlan,
        start: OpIndex,
        tid: TransactionId,
    ) -> Result<Box<dyn OpIterator>, CrustyError> {
        let err = CrustyError::ExecutionError(String::from("Malformed logical plan"));

        // Recursively convert the children in node of physical plan to opiterator.
        let mut children = physical_plan.edges(start).map(|n| {
            Executor::physical_plan_to_op_iterator_helper(
                storage_manager,
                catalog,
                physical_plan,
                n,
                tid,
            )
        });

        // Converts the current node in physical plan to an opiterator.
        let op = physical_plan
            .get_operator(start)
            .ok_or_else(|| err.clone())?;
        let result: Result<Box<dyn OpIterator>, CrustyError> = match op {
            PhysicalOp::Scan(PhysicalScanNode {
                alias,
                container_id,
            }) => match catalog.get_table_id(alias) {
                Some(alias_id) => {
                    let table = catalog.get_table_ptr(alias_id)?;
                    Ok(Box::new(SeqScan::new(
                        storage_manager,
                        table,
                        alias,
                        container_id,
                        tid,
                    )))
                }
                None => Err(CrustyError::CrustyError(format!(
                    "Table {} has no container id ",
                    alias
                ))),
            },
            PhysicalOp::Project(PhysicalProjectNode { identifiers }) => {
                let child = children.next().ok_or_else(|| err.clone())??;
                match &identifiers {
                    ProjectIdentifiers::Wildcard => {
                        let field_indices = (0..child.get_schema().size()).collect::<Vec<usize>>();
                        let project_iterator = ProjectIterator::new(field_indices, child);
                        Ok(Box::new(project_iterator))
                    }
                    ProjectIdentifiers::List(identifiers) => {
                        let (indices, names) =
                            Self::get_field_indices_names(identifiers, child.get_schema())?;
                        let project_iterator =
                            ProjectIterator::new_with_aliases(indices, names, child);
                        Ok(Box::new(project_iterator))
                    }
                }
            }
            PhysicalOp::HashAggregate(PhysicalHashAggregateNode {
                fields, group_by, ..
            }) => {
                let child = children.next().ok_or_else(|| err.clone())??;
                let mut agg_fields = Vec::new();
                let mut ops = Vec::new();
                for field in fields {
                    if let Some(op) = field.agg_op() {
                        ops.push(op);
                        agg_fields.push(field.clone());
                    }
                }
                let (agg_indices, agg_names) =
                    Self::get_field_indices_names(&agg_fields, child.get_schema())?;
                let (groupby_indices, groupby_names) =
                    Self::get_field_indices_names(group_by, child.get_schema())?;
                let agg = Aggregate::new(
                    groupby_indices,
                    groupby_names,
                    agg_indices,
                    agg_names,
                    ops,
                    child,
                );
                Ok(Box::new(agg))
            }
            PhysicalOp::NestedLoopJoin(PhysicalNestedLoopJoinNode {
                left, op, right, ..
            }) => {
                let left_child = children.next().ok_or_else(|| err.clone())??;
                let left_schema = left_child.get_schema();
                let right_child = children.next().ok_or_else(|| err.clone())??;
                let right_schema = right_child.get_schema();

                // Sometimes the join condition is written in reverse of the join tables order.
                if !left_schema.contains(left.column()) {
                    let left_index = Executor::get_field_index(left.column(), right_schema)?;
                    let right_index = Executor::get_field_index(right.column(), left_schema)?;
                    Ok(Box::new(Join::new(
                        op.flip(),
                        left_index,
                        right_index,
                        left_child,
                        right_child,
                    )))
                } else {
                    let left_index = Executor::get_field_index(left.column(), left_schema)?;
                    let right_index = Executor::get_field_index(right.column(), right_schema)?;
                    Ok(Box::new(Join::new(
                        *op,
                        left_index,
                        right_index,
                        left_child,
                        right_child,
                    )))
                }
            }
            PhysicalOp::HashJoin(PhysicalHashJoinNode {
                left, right, op, ..
            }) => {
                let left_child = children.next().ok_or_else(|| err.clone())??;
                let left_schema = left_child.get_schema();
                let right_child = children.next().ok_or_else(|| err.clone())??;
                let right_schema = right_child.get_schema();

                // Sometimes the join condition is written in reverse of the join tables order.
                if !left_schema.contains(left.column()) {
                    let left_index = Executor::get_field_index(left.column(), right_schema)?;
                    let right_index = Executor::get_field_index(right.column(), left_schema)?;
                    Ok(Box::new(HashEqJoin::new(
                        op.flip(),
                        left_index,
                        right_index,
                        left_child,
                        right_child,
                    )))
                } else {
                    let left_index = Executor::get_field_index(left.column(), left_schema)?;
                    let right_index = Executor::get_field_index(right.column(), right_schema)?;
                    Ok(Box::new(HashEqJoin::new(
                        *op,
                        left_index,
                        right_index,
                        left_child,
                        right_child,
                    )))
                }
            }
            PhysicalOp::Filter(PhysicalFilterNode { predicate, .. }) => {
                let child = children.next().ok_or_else(|| err.clone())??;
                let (identifiers, ops, operands, _compound_predicate_op) = match predicate {
                    Predicate::SimplePredicate(SimplePredicate { left, op, right }) => {
                        match (left, right) {
                            (PredExpr::Ident(i), PredExpr::Literal(f)) => {
                                let comp: Option<CompoundPredicateOp> = None;
                                (vec![i.clone()], vec![*op], vec![f.clone()], comp)
                            }
                            (PredExpr::Literal(f), PredExpr::Ident(i)) => {
                                (vec![i.clone()], vec![op.flip()], vec![f.clone()], None)
                            }
                            _ => {
                                return Err(err.clone());
                            }
                        }
                    }
                    Predicate::CompoundPredicate(_) => {
                        //Only supporting simple filter predicates right now
                        unimplemented!()
                    } // Predicate::CompoundPredicate(CompoundPredicate {
                      //     op,
                      //     simple_predicates,
                      // }) => {
                      //     let mut identifiers = Vec::new();
                      //     let mut ops = Vec::new();
                      //     let mut operands = Vec::new();
                      //     for simple_predicate in simple_predicates {
                      //         match (&simple_predicate.left, &simple_predicate.right) {
                      //             (PredExpr::Ident(i), PredExpr::Literal(f)) => {
                      //                 identifiers.push(i.clone());
                      //                 ops.push(simple_predicate.op);
                      //                 operands.push(f.clone());
                      //             }
                      //             (PredExpr::Literal(f), PredExpr::Ident(i)) => {
                      //                 identifiers.push(i.clone());
                      //                 ops.push(simple_predicate.op.flip());
                      //                 operands.push(f.clone());
                      //             }
                      //             _ => {
                      //                 return Err(err.clone());
                      //             }
                      //         }
                      //     }

                      //     (identifiers, ops, operands, Some(op.clone()))
                      // }
                };
                let indexes: Vec<usize> = identifiers
                    .iter()
                    .map(|identifier| {
                        Executor::get_field_index(identifier.column(), child.get_schema()).unwrap()
                    })
                    .collect();
                if identifiers.len() > 1 {
                    todo!(); // fix
                } else if identifiers.is_empty() {
                    Err(CrustyError::ExecutionError(String::from("No predicates")))
                } else {
                    //let idx = Executor::get_field_index(identifier.column(), child.get_schema())?;
                    let filter = Filter::new(
                        *ops.get(0).unwrap(),
                        *indexes.get(0).unwrap(),
                        operands.get(0).unwrap().clone(),
                        child,
                    );
                    Ok(Box::new(filter))
                }
            }
            //MaterializedViews are not required
            PhysicalOp::MaterializedView(_) => unimplemented!(),
            //TODO
            PhysicalOp::SortedAggregate(_) => unimplemented!(),
        };

        if children.next().is_some() {
            Err(err)
        } else {
            result
        }
    }

    /// Get the index of the column in the schema.
    ///
    /// # Arguments
    ///
    /// * `col` - Column name to find the index of.
    /// * `schema` - Schema to look for the column in.
    fn get_field_index(col: &str, schema: &TableSchema) -> Result<usize, CrustyError> {
        schema
            .get_field_index(col)
            .copied()
            .ok_or_else(|| CrustyError::ExecutionError(String::from("Unrecognized column name")))
    }

    // TODO: Fix test cases to be able to address the clippy warning of pointer arguments.
    /// Finds the column indices and names of column alias present in the given schema.
    ///
    /// # Arguments
    ///
    /// * `fields` - Vector of column names to look for.
    /// * `schema` - Schema to look for the column names in.
    #[allow(clippy::ptr_arg)]
    fn get_field_indices_names<'b>(
        fields: &'b Vec<FieldIdentifier>,
        schema: &TableSchema,
    ) -> Result<(Vec<usize>, Vec<&'b str>), CrustyError> {
        let mut field_indices = Vec::new();
        let mut field_names = Vec::new();
        for f in fields.iter() {
            let i = Executor::get_field_index(f.column(), schema)?;
            field_indices.push(i);
            let new_name = f.alias().unwrap_or_else(|| f.column());
            field_names.push(new_name)
        }
        Ok((field_indices, field_names))
    }
}

/* FIXME
#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::*;
    use crate::bufferpool::*;
    use crate::DBSERVER;
    use common::{DataType, Field, TableSchema};

    fn test_logical_plan() -> LogicalPlan {
        let mut lp = LogicalPlan::new();
        let scan = LogicalOp::Scan(ScanNode {
            alias: TABLE_A.to_string(),
        });
        let project = LogicalOp::Project(ProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        });
        let si = lp.add_node(scan);
        let pi = lp.add_node(project);
        lp.add_edge(pi, si);
        lp
    }

    #[test]
    fn test_to_op_iterator() -> Result<(), CrustyError> {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut op = Executor::logical_plan_to_op_iterator(&db, &lp, tid).unwrap();
        op.open()?;
        let mut sum = 0;
        while let Some(t) = op.next()? {
            for i in 0..t.size() {
                sum += match t.get_field(i).unwrap() {
                    Field::IntField(n) => n,
                    _ => panic!("Not an IntField"),
                }
            }
        }
        assert_eq!(sum, TABLE_A_CHECKSUM);
        DBSERVER.transaction_complete(tid, true).unwrap();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_next_not_started() {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.next().unwrap();
    }

    #[test]
    fn test_next() -> Result<(), CrustyError> {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.start()?;
        let mut sum = 0;
        while let Some(t) = executor.next()? {
            for i in 0..t.size() {
                sum += *match t.get_field(i).unwrap() {
                    Field::IntField(n) => n,
                    _ => panic!("Not an IntField"),
                }
            }
        }
        println!("sum: {}", sum);
        assert_eq!(sum, TABLE_A_CHECKSUM);
        DBSERVER.transaction_complete(tid, true).unwrap();
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_close() {
        let db = test_db();
        let lp = test_logical_plan();
        let tid = TransactionId::new();
        let mut executor = Executor::new(&db, &lp, tid).unwrap();
        executor.start().unwrap();
        executor.close().unwrap();
        executor.next().unwrap();
    }

    #[test]
    fn test_get_field_indices_names() -> Result<(), CrustyError> {
        let names = vec!["one", "two", "three", "four"];
        let aliases = vec!["1", "2", "3", "4"];
        let indices = vec![0, 1, 2, 3];
        let types = std::iter::repeat(DataType::Int).take(4).collect();
        let schema = TableSchema::from_vecs(names.clone(), types);

        // Test without aliases.
        let fields = names.iter().map(|s| FieldIdent::new("", s)).collect();
        let (actual_indices, actual_names) = Executor::get_field_indices_names(&fields, &schema)?;
        assert_eq!(actual_indices, indices);
        assert_eq!(actual_names, names);

        // Test with aliases.
        let fields = names
            .iter()
            .zip(aliases.iter())
            .map(|(n, a)| FieldIdent::new_column_alias("", n, a))
            .collect();
        let (actual_indices, actual_names) = Executor::get_field_indices_names(&fields, &schema)?;
        assert_eq!(actual_indices, indices);
        assert_eq!(actual_names, aliases);
        Ok(())
    }
}*/
