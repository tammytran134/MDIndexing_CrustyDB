use common::catalog::Catalog;
use common::ids::StateType;
use common::logical_plan::*;
use common::physical_plan::*;
use common::CrustyError;

pub struct Optimizer {}

#[allow(clippy::new_without_default)]
impl Optimizer {
    #[allow(clippy::let_and_return)]
    pub fn new() -> Optimizer {
        let sm = Optimizer {};
        sm
    }

    /// Converts a logical operator into a physical operator
    ///
    /// # Arguments
    ///
    /// * `logical_op` - the logical operator to convert to a physical operator
    /// * `physical_plan` - the physical plan to which the converted logical op will be added
    /// * `catalog` - the catalog in which containers can be created during this conversion
    fn logical_op_to_physical_op<T: Catalog>(
        &self,
        logical_op: LogicalOp,
        physical_plan: &mut PhysicalPlan,
        catalog: &T,
    ) -> Result<PhysicalOp, CrustyError> {
        match logical_op {
            LogicalOp::Scan(ScanNode {
                alias,
                container_id,
            }) => {
                physical_plan.add_base_table(container_id);
                Ok(PhysicalOp::Scan(PhysicalScanNode {
                    alias,
                    container_id,
                }))
            }
            LogicalOp::Project(ProjectNode { identifiers }) => {
                Ok(PhysicalOp::Project(PhysicalProjectNode { identifiers }))
            }
            LogicalOp::Aggregate(AggregateNode { fields, group_by }) => {
                // Creating a hash-table with the storage manager. Only needed if persisting the hash table for views.
                // TODOadd name?
                let hash_table_state_id =
                    catalog.get_new_container_id(StateType::HashTable, None)?;
                physical_plan.add_hash_table(hash_table_state_id);
                // Create hash aggregate node.
                Ok(PhysicalOp::HashAggregate(PhysicalHashAggregateNode {
                    hash_table_state_id,
                    hash_table_key: group_by.clone(),
                    fields,
                    group_by,
                }))
            }
            LogicalOp::Join(JoinNode {
                left,
                right,
                op,
                left_table,
                right_table,
            }) => {
                Ok(PhysicalOp::NestedLoopJoin(PhysicalNestedLoopJoinNode {
                    left,
                    right,
                    op,
                    left_table,
                    right_table,
                }))
            },
            LogicalOp::Filter(FilterNode { table, predicate }) => {
                Ok(PhysicalOp::Filter(PhysicalFilterNode { table, predicate }))
            }
            //not currently covering read delta and write delta logical ops
            _ => todo!(),
        }
    }

    /// Converts a logical plan into a physical plan
    ///
    /// # Arguments
    ///
    /// * `logical_plan` - the logical plan to convert to a physical plan
    /// * `catalog` - the catalog in which containers can be created during this conversion  
    pub fn logical_plan_to_physical_plan<T: Catalog>(
        &self,
        logical_plan: LogicalPlan,
        catalog: &T,
        is_mat_view: bool,
    ) -> Result<PhysicalPlan, CrustyError> {
        let mut physical_plan = PhysicalPlan::new();
        for (idx, node) in logical_plan.node_references() {
            let logical_op = node.data();
            let physical_op =
                self.logical_op_to_physical_op(logical_op.clone(), &mut physical_plan, catalog)?;
            physical_plan.add_node(physical_op);
            if !is_mat_view {
                logical_plan.root();
                if let Some(root_idx) = logical_plan.root() {
                    if idx == root_idx {
                        physical_plan.set_root(idx)?;
                    }
                }
            }
        }

        for edge in logical_plan.edge_references() {
            physical_plan.add_edge(edge.source(), edge.target())
        }

        if is_mat_view {
            // add name?
            let materialized_view_state_id =
                catalog.get_new_container_id(StateType::MatView, None)?;
            let materialized_view_node_index =
                physical_plan.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
                    materialized_view_state_id,
                }));
            physical_plan.set_root(materialized_view_node_index)?;

            match logical_plan.root() {
                Some(logical_root_index) => {
                    physical_plan.add_edge(materialized_view_node_index, logical_root_index);
                }
                None => {
                    return Err(CrustyError::CrustyError(String::from(
                        "logical plan converted to physical plan must have root",
                    )));
                }
            }
        }

        Ok(physical_plan)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use common::database::Database;

    fn logical_plan1() -> LogicalPlan {
        LogicalPlan::from_json("{\"edges\":{\"1\":[\"0\"]},\"nodes\":{\"0\":{\"Scan\":{\"alias\":\"test\",\"container_id\":0,\"timestamp\":0}},\"1\":{\"Project\":{\"identifiers\":\"Wildcard\"}}},\"root\":\"1\"}").unwrap()
    }

    fn logical_plan2() -> LogicalPlan {
        LogicalPlan::from_json("{\"edges\":{\"1\":[\"0\"],\"2\":[\"1\"],\"3\":[\"2\"]},\"nodes\":{\"0\":{\"Scan\":{\"alias\":\"test\",\"container_id\":0,\"timestamp\":0}},\"1\":{\"Filter\":{\"predicate\":{\"SimplePredicate\":{\"left\":{\"Ident\":{\"alias\":\"b\",\"column\":\"test.b\",\"op\":null,\"table\":\"test\"}},\"op\":\"Equals\",\"right\":{\"Literal\":{\"IntField\":2}}}},\"table\":\"test\"}},\"2\":{\"Aggregate\":{\"fields\":[{\"alias\":\"sum_test.a\",\"column\":\"test.a\",\"op\":\"Sum\",\"table\":\"test\"}],\"group_by\":[]}},\"3\":{\"Project\":{\"identifiers\":{\"List\":[{\"alias\":null,\"column\":\"sum_test.a\",\"op\":null,\"table\":\"test\"}]}}}},\"root\":\"3\"}").unwrap()
    }

    fn logical_plan3() -> LogicalPlan {
        LogicalPlan::from_json("{\"edges\":{\"1\":[\"0\"],\"2\":[\"1\"]},\"nodes\":{\"0\":{\"Scan\":{\"alias\":\"test\",\"container_id\":0,\"timestamp\":0}},\"1\":{\"Filter\":{\"predicate\":{\"CompoundPredicate\":{\"op\":\"And\",\"simple_predicates\":[{\"left\":{\"Ident\":{\"alias\":\"a\",\"column\":\"test.a\",\"op\":null,\"table\":\"test\"}},\"op\":\"Equals\",\"right\":{\"Literal\":{\"IntField\":4}}},{\"left\":{\"Ident\":{\"alias\":\"b\",\"column\":\"test.b\",\"op\":null,\"table\":\"test\"}},\"op\":\"Equals\",\"right\":{\"Literal\":{\"IntField\":2}}}]}},\"table\":\"test\"}},\"2\":{\"Project\":{\"identifiers\":\"Wildcard\"}}},\"root\":\"2\"}").unwrap()
    }

    #[test]
    fn test_mat_view_from_logical() {
        let db = Database::new(String::from("test"));
        let opt = Optimizer::new();
        let physical_plan = opt
            .logical_plan_to_physical_plan(logical_plan1(), &db, true)
            .unwrap();
        assert_eq!(physical_plan.node_count(), 3);
        assert_eq!(physical_plan.edge_count(), 2);
        assert!(physical_plan.all_reachable_from_root().unwrap());
        assert!(physical_plan.cycle_free());
        if let PhysicalOp::MaterializedView(_) = physical_plan
            .get_operator(physical_plan.root().unwrap())
            .unwrap()
        {
        } else {
            panic!("Incorrect root")
        }
        assert_eq!(physical_plan.base_tables().len(), 1);
        assert_eq!(physical_plan.hash_tables().len(), 0);

        let physical_plan = opt
            .logical_plan_to_physical_plan(logical_plan2(), &db, true)
            .unwrap();
        assert_eq!(physical_plan.node_count(), 5);
        assert_eq!(physical_plan.edge_count(), 4);
        assert!(physical_plan.all_reachable_from_root().unwrap());
        assert!(physical_plan.cycle_free());
        if let PhysicalOp::MaterializedView(_) = physical_plan
            .get_operator(physical_plan.root().unwrap())
            .unwrap()
        {
        } else {
            panic!("Incorrect root")
        }
        assert_eq!(physical_plan.base_tables().len(), 1);
        assert_eq!(physical_plan.hash_tables().len(), 1);

        let physical_plan = opt
            .logical_plan_to_physical_plan(logical_plan3(), &db, true)
            .unwrap();
        assert_eq!(physical_plan.node_count(), 4);
        assert_eq!(physical_plan.edge_count(), 3);
        assert!(physical_plan.all_reachable_from_root().unwrap());
        assert!(physical_plan.cycle_free());
        if let PhysicalOp::MaterializedView(_) = physical_plan
            .get_operator(physical_plan.root().unwrap())
            .unwrap()
        {
        } else {
            panic!("Incorrect root")
        }
        assert_eq!(physical_plan.base_tables().len(), 1);
        assert_eq!(physical_plan.hash_tables().len(), 0);
    }
}
