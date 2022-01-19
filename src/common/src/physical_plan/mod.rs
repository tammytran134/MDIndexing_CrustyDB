use std::collections::{HashMap, HashSet};
use std::fmt;

use serde_json::{json, Value};

use crate::crusty_graph::{CrustyGraph, NodeIndex};
use crate::ids::ContainerId;
use crate::logical_plan::OpIndex;
use crate::CrustyError;

pub use physical_op::*;

mod physical_op;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PhysicalOp {
    Scan(PhysicalScanNode),
    Project(PhysicalProjectNode),
    HashAggregate(PhysicalHashAggregateNode),
    SortedAggregate(PhysicalSortedAggregateNode),
    NestedLoopJoin(PhysicalNestedLoopJoinNode),
    HashJoin(PhysicalHashJoinNode),
    Filter(PhysicalFilterNode),
    MaterializedView(MaterializedViewNode),
}

/// Graph where nodes represent physical operations and edges represent the flow of data.
pub struct PhysicalPlan {
    /// Graph of the Physical plan.
    dataflow: CrustyGraph<PhysicalOp>,
    /// The root represents final output operation. Root does not work if the graph contains any unconnected components.
    root: Option<OpIndex>,
    /// The container ids of all the base tables used in this physical plan
    base_tables: Vec<ContainerId>,
    /// The container ids of all the hash tables used in this physical plan
    hash_tables: Vec<ContainerId>,
}

impl Default for PhysicalPlan {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PhysicalPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_fmt(format_args!(
            "Root {:?} \n dataflow: {:?}",
            self.root, self.dataflow
        ))
    }
}

impl PhysicalPlan {
    /// Creates an empty physical plan.
    pub fn new() -> Self {
        Self {
            dataflow: CrustyGraph::new(),
            root: None,
            base_tables: Vec::new(),
            hash_tables: Vec::new(),
        }
    }

    pub fn add_base_table(&mut self, base_table_id: ContainerId) {
        // check to see if vec already contains this base table
        self.base_tables.push(base_table_id);
    }

    pub fn add_hash_table(&mut self, hash_table_id: ContainerId) {
        // check to see if vec already contains this base table
        self.hash_tables.push(hash_table_id);
    }

    pub fn base_tables(&self) -> &Vec<ContainerId> {
        &self.base_tables
    }

    pub fn hash_tables(&self) -> &Vec<ContainerId> {
        &self.hash_tables
    }

    /// Adds a node with an associated PhysicalOp to the Physical plan and returns the index of the added node.
    ///
    /// # Arguments
    ///
    /// * `operator` - Operator to add to the Physical plan.
    pub fn add_node(&mut self, operator: PhysicalOp) -> OpIndex {
        self.dataflow.add_node(operator)
    }

    /// Adds from source to target.
    ///
    /// In the Physical plan representation data flows from target to source.
    ///
    /// # Arguments
    ///
    /// * `source` - Data producer.
    /// * `target` - Data consumer.
    pub fn add_edge(&mut self, source: OpIndex, target: OpIndex) {
        self.dataflow.add_edge(source, target);
    }

    /// Returns an iterator over all nodes that 'from' has an edge to.
    ///
    /// # Arguments
    ///
    /// * `from` - Node to get the edges of.
    // TODO(williamma12): Check if these lifetimes are necessary or not.
    #[allow(clippy::needless_lifetimes)]
    pub fn edges<'a>(&'a self, from: OpIndex) -> impl Iterator<Item = NodeIndex> + 'a {
        self.dataflow.edges(from)
    }

    /// Gets the index of the root node, if such a node is present.
    ///
    /// The root node represents the final output operation in the physical plan.
    pub fn root(&self) -> Option<OpIndex> {
        self.root
    }

    /// Returns the PhysicalOp associated with a node.
    ///
    /// # Arguments
    ///
    /// * `index` - Index of the node to get the physical operation of.
    pub fn get_operator(&self, index: OpIndex) -> Option<&PhysicalOp> {
        self.dataflow.node_data(index)
    }

    /// Returns the total number of nodes present in the graph.
    pub fn node_count(&self) -> usize {
        self.dataflow.node_count()
    }

    /// Returns the total number of edges present in the graph.
    pub fn edge_count(&self) -> usize {
        self.dataflow.edge_count()
    }

    /// Sets the root of a physical plan to the given op index
    ///
    /// # Arguments
    ///
    /// * `index` - the index to set the root to
    pub fn set_root(&mut self, index: OpIndex) -> Result<(), CrustyError> {
        if self.root.is_none() {
            self.root = Some(index);
            Ok(())
        } else {
            Err(CrustyError::CrustyError(String::from(
                "Attempted to set root of physical plan with root",
            )))
        }
    }

    /// Serializes the Physical Plan as json.
    pub fn to_json(&self) -> serde_json::Value {
        let mut node_map = HashMap::new();
        let mut edge_map = HashMap::new();
        for (i, node) in self.dataflow.node_references() {
            node_map.insert(i, node.data());
        }
        for (_, edge) in self.dataflow.edge_references().enumerate() {
            let source = edge.source();
            let targets = edge_map.entry(source).or_insert_with(Vec::new);
            targets.push(edge.target().to_string());
        }
        return json!({"nodes":node_map,
                      "edges":edge_map,
                      "root":self.root.map(|i| i.to_string()),
                      "base_tables":self.base_tables,
                      "hash_tables":self.hash_tables});
    }

    fn map_crusty_err<T>(
        result: serde_json::Result<T>,
        err: CrustyError,
    ) -> Result<T, CrustyError> {
        match result {
            Ok(res) => Ok(res),
            _ => Err(err),
        }
    }

    /// De-Serializes a json representation of the Physical Plan created in to_json
    pub fn from_json(json: &str) -> Result<Self, CrustyError> {
        let malformed_err =
            CrustyError::CrustyError(String::from("Malformatted Physical plan json"));
        let v: Value =
            PhysicalPlan::map_crusty_err(serde_json::from_str(json), malformed_err.clone())?;
        let nodes: HashMap<String, PhysicalOp> = PhysicalPlan::map_crusty_err(
            serde_json::from_value(v["nodes"].clone()),
            malformed_err.clone(),
        )?;
        let edges: HashMap<String, Vec<String>> = PhysicalPlan::map_crusty_err(
            serde_json::from_value(v["edges"].clone()),
            malformed_err.clone(),
        )?;
        let root: Option<String> = PhysicalPlan::map_crusty_err(
            serde_json::from_value(v["root"].clone()),
            malformed_err.clone(),
        )?;
        let base_tables: Vec<ContainerId> = PhysicalPlan::map_crusty_err(
            serde_json::from_value(v["base_tables"].clone()),
            malformed_err.clone(),
        )?;
        let hash_tables: Vec<ContainerId> = PhysicalPlan::map_crusty_err(
            serde_json::from_value(v["hash_tables"].clone()),
            malformed_err.clone(),
        )?;

        let mut graph_map = HashMap::new();
        let mut plan = PhysicalPlan::new();
        for (i, val) in nodes.iter() {
            let node = plan.add_node(val.clone());
            graph_map.insert(i, node);
        }

        if let Some(i) = root {
            let root_node = graph_map.get(&i).ok_or_else(|| malformed_err.clone())?;
            plan.root = Some(*root_node);
        }

        for (source, targets) in edges.iter() {
            let source_node = graph_map.get(source).ok_or_else(|| malformed_err.clone())?;
            for target in targets {
                let target_node = graph_map
                    .get(&target.to_string())
                    .ok_or_else(|| malformed_err.clone())?;
                plan.add_edge(*source_node, *target_node);
            }
        }

        plan.base_tables = base_tables;
        plan.hash_tables = hash_tables;

        if !plan.cycle_free() {
            return Err(CrustyError::CrustyError(String::from(
                "Physical plan Plan loaded from json contains a cycle",
            )));
        }

        if !plan.all_reachable_from_root()? {
            return Err(CrustyError::CrustyError(String::from(
                "Physical Plan loaded from json contains nodes not reachable from root",
            )));
        }

        Ok(plan)
    }

    /// Checks if the Physical plan has a cycle
    /// if this has a cycle, the query could run forever
    pub fn cycle_free(&self) -> bool {
        self.dataflow.cycle_free()
    }

    /// Checks if all nodes in the operator graph are reachable from the root
    /// Raises error if the Physical plan has no root
    pub fn all_reachable_from_root(&self) -> Result<bool, CrustyError> {
        match self.root() {
            Some(node) => self.dataflow.all_reachable_from_node(node),
            None => Err(CrustyError::CrustyError(String::from(
                "Physical plan loaded from json has no root",
            ))),
        }
    }

    /// Returns the container id used by a node, if it exists
    /// # Arguments
    ///
    /// * `op_index` - the index of the node to get the container of
    fn get_container_id(&self, op_index: OpIndex) -> Option<ContainerId> {
        match self.get_operator(op_index) {
            Some(PhysicalOp::MaterializedView(MaterializedViewNode {
                materialized_view_state_id,
            })) => Some(*materialized_view_state_id),
            Some(PhysicalOp::HashAggregate(PhysicalHashAggregateNode {
                hash_table_state_id,
                ..
            })) => Some(*hash_table_state_id),
            Some(PhysicalOp::HashJoin(PhysicalHashJoinNode {
                hash_table_state_id,
                ..
            })) => Some(*hash_table_state_id),
            Some(PhysicalOp::Scan(PhysicalScanNode { container_id, .. })) => Some(*container_id),
            _ => None,
        }
    }

    /// Returns the container id of the output
    pub fn get_output_container_id(&self) -> Result<ContainerId, CrustyError> {
        match self.root() {
            Some(root_index) => {
                if let PhysicalOp::MaterializedView(MaterializedViewNode {
                    materialized_view_state_id,
                }) = self.get_operator(root_index).unwrap()
                {
                    Ok(*materialized_view_state_id)
                } else {
                    Err(CrustyError::CrustyError(String::from(
                        "root of physical plan is not a materialized view node",
                    )))
                }
            }
            None => Err(CrustyError::CrustyError(String::from(
                "attempted to get output containter id of physical plan with no root",
            ))),
        }
    }

    /// Returns all dependencies of a container in this plan
    ///
    /// # Arguments
    ///
    /// * `container_id` - the container to get the dependencies of
    pub fn get_dependencies(
        &self,
        container_id: ContainerId,
    ) -> Result<Option<Vec<ContainerId>>, CrustyError> {
        match self.root() {
            Some(root_index) => {
                let dependencies = self.get_dependencies_helper(root_index, container_id);
                if dependencies.contains(&container_id) {
                    Err(CrustyError::CrustyError(String::from(
                        "container is dependent on itself",
                    )))
                } else {
                    let res: Vec<ContainerId> = dependencies.into_iter().collect();
                    if res.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(res))
                    }
                }
            }
            None => Err(CrustyError::CrustyError(String::from(
                "attempted to get dependencies of physical plan with no root",
            ))),
        }
    }

    /// Returns all dependencies of a container in the subgraph reachable from an operator
    ///
    /// # Arguments
    ///
    /// * `op_index` - the operator whose reachable subgraph we are checking for dependencies
    /// * `container_id` - the container to get the dependencies of
    fn get_dependencies_helper(
        &self,
        op_index: OpIndex,
        container_id: ContainerId,
    ) -> HashSet<ContainerId> {
        let mut res = HashSet::new();

        // this node has the container we want to get the dependencies of
        if self.get_container_id(op_index) == Some(container_id) {
            for child in self.edges(op_index) {
                let child_containers = self.get_containers_in_children(child);
                res.extend(child_containers);
            }
        // this node does not have the container we want to get the dependencies of
        } else {
            for child in self.edges(op_index) {
                let child_containers = self.get_dependencies_helper(child, container_id);
                res.extend(child_containers);
            }
        }
        res
    }

    /// Returns all containers in the subgraph reachable from an operator
    ///
    /// # Arguments
    ///
    /// * `op_index` - the operator whose reachable subgraph we are checking for containers
    fn get_containers_in_children(&self, op_index: OpIndex) -> HashSet<ContainerId> {
        let mut res = HashSet::new();
        if let Some(container_id) = self.get_container_id(op_index) {
            res.insert(container_id);
        }

        for child in self.edges(op_index) {
            let child_containers = self.get_containers_in_children(child);
            res.extend(child_containers);
        }
        res
    }
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_json())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{
        FieldIdentifier, PredExpr, Predicate, ProjectIdentifiers, SimplePredicate,
        SimplePredicateOp,
    };
    use crate::Field;

    #[test]
    fn test_new() {
        let physical_plan = PhysicalPlan::new();
        assert_eq!(physical_plan.node_count(), 0);
        assert_eq!(physical_plan.edge_count(), 0);
        assert_eq!(physical_plan.root(), None);
    }

    #[test]
    fn test_add_node() {
        let count = 10;
        let mut physical_plan = PhysicalPlan::new();
        for i in 0..count {
            physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
                alias: i.to_string(),
                container_id: 1,
            }));
        }
        assert_eq!(physical_plan.node_count(), count);
    }

    #[test]
    fn test_add_edge() {
        let count = 10;
        let mut physical_plan = PhysicalPlan::new();
        let mut prev = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: 0.to_string(),
            container_id: 0,
        }));
        for i in 0..count {
            let curr = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
                alias: i.to_string(),
                container_id: i as ContainerId,
            }));
            physical_plan.add_edge(curr, prev);
            prev = curr;
            if i == count - 1 {
                physical_plan.set_root(prev).unwrap();
            }
        }
        assert_eq!(physical_plan.root(), Some(prev));
        assert_eq!(physical_plan.edge_count(), count);
    }

    #[test]
    fn test_add_two_edges() {
        let mut physical_plan = PhysicalPlan::new();
        let parent = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("parent"),
            container_id: 0,
        }));
        let child1 = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("child1"),
            container_id: 1,
        }));
        let child2 = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("child2"),
            container_id: 2,
        }));
        physical_plan.add_edge(parent, child1);
        physical_plan.add_edge(parent, child2);
        assert_eq!(physical_plan.edge_count(), 2);
    }

    #[test]
    fn test_edges() {
        let mut physical_plan = PhysicalPlan::new();
        let parent = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("parent"),
            container_id: 0,
        }));
        let child1 = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("child1"),
            container_id: 1,
        }));
        let child2 = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("child2"),
            container_id: 2,
        }));
        physical_plan.add_edge(parent, child1);
        physical_plan.add_edge(parent, child2);
        let mut edges = physical_plan.edges(parent);
        assert_eq!(edges.next(), Some(child2));
        assert_eq!(edges.next(), Some(child1));
    }

    #[test]
    fn test_get_operator() {
        let count = 5;
        let mut nodes = Vec::new();
        let mut physical_plan = PhysicalPlan::new();
        for i in 0..count {
            let index = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
                alias: i.to_string(),
                container_id: i as ContainerId,
            }));
            nodes.push(index);
        }

        for (i, &node) in nodes.iter().enumerate().take(count) {
            let expected = i.to_string();
            match physical_plan.get_operator(node) {
                Some(PhysicalOp::Scan(s)) => {
                    assert_eq!(expected, s.alias);
                }
                _ => panic!("Incorrect operator"),
            }
        }
    }

    #[test]
    fn test_json() {
        let mut physical_plan = PhysicalPlan::new();
        let scan = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("Table"),
            container_id: 0,
        }));

        let project = physical_plan.add_node(PhysicalOp::Project(PhysicalProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));

        let mat_view = physical_plan.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
            materialized_view_state_id: 0,
        }));
        physical_plan.set_root(mat_view).unwrap();

        physical_plan.add_edge(project, scan);
        physical_plan.add_edge(mat_view, project);

        let json = physical_plan.to_json();
        let new_physical_plan = PhysicalPlan::from_json(&json.to_string()).unwrap();
        assert_eq!(physical_plan.node_count(), new_physical_plan.node_count());
        assert_eq!(physical_plan.edge_count(), new_physical_plan.edge_count());

        let original_root = physical_plan
            .get_operator(physical_plan.root().unwrap())
            .unwrap();
        let new_root = new_physical_plan
            .get_operator(new_physical_plan.root().unwrap())
            .unwrap();
        match (original_root, new_root) {
            (PhysicalOp::MaterializedView(_), PhysicalOp::MaterializedView(_)) => (),
            _ => panic!(
                "Incorrect root: original {:?}, new {:?}",
                original_root, new_root
            ),
        }
        assert_eq!(
            physical_plan.base_tables().len(),
            new_physical_plan.base_tables().len()
        );
        assert_eq!(
            physical_plan.hash_tables().len(),
            new_physical_plan.hash_tables().len()
        );
    }

    #[test]
    fn test_cycle_free() {
        let mut physical_plan = PhysicalPlan::new();
        let scan = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("Table"),
            container_id: 0,
        }));
        let project = physical_plan.add_node(PhysicalOp::Project(PhysicalProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));

        let mat_view = physical_plan.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
            materialized_view_state_id: 0,
        }));
        physical_plan.set_root(mat_view).unwrap();

        physical_plan.add_edge(project, scan);
        assert!(physical_plan.cycle_free());

        physical_plan.add_edge(mat_view, project);
        assert!(physical_plan.cycle_free());

        physical_plan.add_edge(mat_view, scan);
        assert!(physical_plan.cycle_free());

        physical_plan.add_edge(scan, mat_view);
        assert!(!physical_plan.cycle_free());
    }

    #[test]
    fn test_all_reachable_from_root() {
        let mut physical_plan = PhysicalPlan::new();
        assert!(physical_plan.all_reachable_from_root().is_err());

        let scan = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("Table"),
            container_id: 0,
        }));
        assert!(physical_plan.all_reachable_from_root().is_err());

        let project = physical_plan.add_node(PhysicalOp::Project(PhysicalProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));
        assert!(physical_plan.all_reachable_from_root().is_err());

        let mat_view = physical_plan.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
            materialized_view_state_id: 0,
        }));
        assert!(physical_plan.all_reachable_from_root().is_err());
        physical_plan.set_root(mat_view).unwrap();
        assert!(!physical_plan.all_reachable_from_root().unwrap());

        physical_plan.add_edge(project, scan);
        assert!(!physical_plan.all_reachable_from_root().unwrap());

        physical_plan.add_edge(mat_view, project);
        assert!(physical_plan.all_reachable_from_root().unwrap());

        physical_plan.add_edge(mat_view, scan);
        assert!(physical_plan.all_reachable_from_root().unwrap());

        physical_plan.add_edge(scan, mat_view);
        assert!(physical_plan.all_reachable_from_root().unwrap());
    }

    #[test]
    fn test_get_container_id() {
        let mut physical_plan = PhysicalPlan::new();

        let scan = physical_plan.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("Table"),
            container_id: 0,
        }));

        let project = physical_plan.add_node(PhysicalOp::Project(PhysicalProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));

        let mat_view = physical_plan.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
            materialized_view_state_id: 1,
        }));
        assert!(physical_plan.get_output_container_id().is_err());

        physical_plan.set_root(mat_view).unwrap();
        physical_plan.add_edge(project, scan);
        physical_plan.add_edge(mat_view, project);

        assert_eq!(physical_plan.get_container_id(scan), Some(0));
        assert_eq!(physical_plan.get_container_id(project), None);
        assert_eq!(physical_plan.get_container_id(mat_view), Some(1));
        assert_eq!(physical_plan.get_output_container_id(), Ok(1));
    }

    #[test]
    fn test_get_dependencies() {
        let mut physical_plan1 = PhysicalPlan::new();

        let scan = physical_plan1.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("Table"),
            container_id: 0,
        }));

        let project = physical_plan1.add_node(PhysicalOp::Project(PhysicalProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));

        let mat_view =
            physical_plan1.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
                materialized_view_state_id: 1,
            }));

        assert!(physical_plan1.get_dependencies(0).is_err());
        assert!(physical_plan1.get_dependencies(1).is_err());

        physical_plan1.set_root(mat_view).unwrap();
        physical_plan1.add_edge(project, scan);
        physical_plan1.add_edge(mat_view, project);

        assert!(physical_plan1.get_dependencies(0).unwrap().is_none());
        assert_eq!(
            physical_plan1.get_dependencies(1).unwrap().unwrap(),
            vec![0]
        );

        // case with same state used in several places
        let mut physical_plan2 = PhysicalPlan::new();

        let a_scan1 = physical_plan2.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("a"),
            container_id: 0,
        }));
        let a_scan2 = physical_plan2.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("a"),
            container_id: 0,
        }));

        let b_scan1 = physical_plan2.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("b"),
            container_id: 1,
        }));
        let b_scan2 = physical_plan2.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("b"),
            container_id: 1,
        }));

        let c_scan = physical_plan2.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("c"),
            container_id: 2,
        }));

        let ab_join1 = physical_plan2.add_node(PhysicalOp::HashJoin(PhysicalHashJoinNode {
            left: FieldIdentifier::new("a", "acol"),
            right: FieldIdentifier::new("b", "bcol"),
            op: SimplePredicateOp::Equals,
            left_table: Some(String::from("a")),
            right_table: Some(String::from("b")),
            hash_table_state_id: 3,
            hash_table_key: FieldIdentifier::new("a", "acol"),
        }));
        let ab_join2 = physical_plan2.add_node(PhysicalOp::HashJoin(PhysicalHashJoinNode {
            left: FieldIdentifier::new("a", "acol"),
            right: FieldIdentifier::new("b", "bcol"),
            op: SimplePredicateOp::Equals,
            left_table: Some(String::from("a")),
            right_table: Some(String::from("b")),
            hash_table_state_id: 3,
            hash_table_key: FieldIdentifier::new("a", "acol"),
        }));

        let abc_join = physical_plan2.add_node(PhysicalOp::HashJoin(PhysicalHashJoinNode {
            left: FieldIdentifier::new("a", "acol"),
            right: FieldIdentifier::new("c", "ccol"),
            op: SimplePredicateOp::Equals,
            left_table: Some(String::from("a")),
            right_table: Some(String::from("b")),
            hash_table_state_id: 4,
            hash_table_key: FieldIdentifier::new("a", "acol"),
        }));

        let aggregate =
            physical_plan2.add_node(PhysicalOp::HashAggregate(PhysicalHashAggregateNode {
                fields: Vec::new(),
                group_by: Vec::new(),
                hash_table_state_id: 5,
                hash_table_key: Vec::new(),
            }));

        let filter = physical_plan2.add_node(PhysicalOp::Filter(PhysicalFilterNode {
            table: String::from("filtered"),
            predicate: Predicate::SimplePredicate(SimplePredicate {
                left: PredExpr::Literal(Field::IntField(5)),
                op: SimplePredicateOp::Equals,
                right: PredExpr::Literal(Field::IntField(5)),
            }),
        }));

        let all_join = physical_plan2.add_node(PhysicalOp::HashJoin(PhysicalHashJoinNode {
            left: FieldIdentifier::new("a", "acol"),
            right: FieldIdentifier::new("a", "acol"),
            op: SimplePredicateOp::Equals,
            left_table: Some(String::from("a")),
            right_table: Some(String::from("b")),
            hash_table_state_id: 6,
            hash_table_key: FieldIdentifier::new("a", "acol"),
        }));

        let project = physical_plan2.add_node(PhysicalOp::Project(PhysicalProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));

        let mat_view =
            physical_plan2.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
                materialized_view_state_id: 7,
            }));
        physical_plan2.set_root(mat_view).unwrap();

        physical_plan2.add_edge(mat_view, project);
        physical_plan2.add_edge(project, all_join);
        physical_plan2.add_edge(all_join, aggregate);
        physical_plan2.add_edge(aggregate, ab_join1);
        physical_plan2.add_edge(ab_join1, a_scan1);
        physical_plan2.add_edge(ab_join1, b_scan1);
        physical_plan2.add_edge(all_join, filter);
        physical_plan2.add_edge(filter, abc_join);
        physical_plan2.add_edge(abc_join, c_scan);
        physical_plan2.add_edge(abc_join, ab_join2);
        physical_plan2.add_edge(ab_join2, a_scan2);
        physical_plan2.add_edge(ab_join2, b_scan2);

        let dependencies0 = physical_plan2.get_dependencies(0).unwrap();
        assert!(dependencies0.is_none());

        let dependencies1 = physical_plan2.get_dependencies(1).unwrap();
        assert!(dependencies1.is_none());

        let dependencies2 = physical_plan2.get_dependencies(2).unwrap();
        assert!(dependencies2.is_none());

        let mut dependencies3 = physical_plan2.get_dependencies(3).unwrap().unwrap();
        dependencies3.sort_unstable();
        assert_eq!(dependencies3, vec![0, 1]);

        let mut dependencies4 = physical_plan2.get_dependencies(4).unwrap().unwrap();
        dependencies4.sort_unstable();
        assert_eq!(dependencies4, vec![0, 1, 2, 3]);

        let mut dependencies5 = physical_plan2.get_dependencies(5).unwrap().unwrap();
        dependencies5.sort_unstable();
        assert_eq!(dependencies5, vec![0, 1, 3]);

        let mut dependencies6 = physical_plan2.get_dependencies(6).unwrap().unwrap();
        dependencies6.sort_unstable();
        assert_eq!(dependencies6, vec![0, 1, 2, 3, 4, 5]);

        let mut dependencies7 = physical_plan2.get_dependencies(7).unwrap().unwrap();
        dependencies7.sort_unstable();
        assert_eq!(dependencies7, vec![0, 1, 2, 3, 4, 5, 6]);

        // cyclic dependency case
        let mut physical_plan3 = PhysicalPlan::new();

        let scan = physical_plan3.add_node(PhysicalOp::Scan(PhysicalScanNode {
            alias: String::from("Table"),
            container_id: 0,
        }));

        let project = physical_plan3.add_node(PhysicalOp::Project(PhysicalProjectNode {
            identifiers: ProjectIdentifiers::Wildcard,
        }));

        let mat_view =
            physical_plan3.add_node(PhysicalOp::MaterializedView(MaterializedViewNode {
                materialized_view_state_id: 0,
            }));

        physical_plan3.set_root(mat_view).unwrap();
        physical_plan3.add_edge(project, scan);
        physical_plan3.add_edge(mat_view, project);

        assert!(physical_plan3.get_dependencies(0).is_err());
    }
}
