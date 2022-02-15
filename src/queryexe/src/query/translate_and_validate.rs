use common::catalog::Catalog;
use common::logical_plan::*;
use common::{get_name, CrustyError, DataType, Field, SimplePredicateOp};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, JoinConstraint, JoinOperator, SelectItem, SetExpr,
    TableFactor, Value,
};
use std::collections::HashSet;

/// Translates input to a LogicalPlan
/// Validates the columns and tables referenced using the catalog
/// Shares lifetime 'a with catalog
pub struct TranslateAndValidate<'a, T: Catalog> {
    /// Logical plan of operators encountered so far.
    plan: LogicalPlan,
    /// Catalog to validate the translations.
    catalog: &'a T,
    /// List of tables encountered. Used for field validation.
    tables: Vec<String>,
}

impl<'a, T: 'a + Catalog> TranslateAndValidate<'a, T> {
    /// Creates a new TranslateAndValidate object.
    fn new(catalog: &'a T) -> Self {
        Self {
            plan: LogicalPlan::new(),
            catalog,
            tables: Vec::new(),
        }
    }

    /// Given a column name, try to figure out what table it belongs to by looking through all of the tables.
    ///
    /// # Arguments
    ///
    /// * `identifiers` - a list of elements in a multi-part identifier e.g. table.column would be vec!["table", "column"]
    ///
    /// # Returns
    ///
    /// FieldIdent's of the form { table: table, column: table.column, alias: column }
    /// or { table: table, column: table.column} if the full identifier is passed.
    fn disambiguate_name(&self, identifiers: Vec<&str>) -> Result<FieldIdentifier, CrustyError> {
        let orig = identifiers.join(".");
        if identifiers.len() > 2 {
            return Err(CrustyError::ValidationError(format!(
                "No . table names supported in field {}",
                orig
            )));
        }
        if identifiers.len() == 2 {
            let table_id = self
                .catalog
                .get_table_id(&identifiers[0].to_string())
                .ok_or_else(|| CrustyError::CrustyError("Missing Table".to_string()))?;
            if self.catalog.is_valid_column(table_id, identifiers[1]) {
                return Ok(FieldIdentifier::new(identifiers[0], &orig));
            }
            return Err(CrustyError::ValidationError(format!(
                "The field {} is not present in tables listed in the query",
                orig
            )));
        }

        let mut field = None;
        for table in &self.tables {
            let table_id = self
                .catalog
                .get_table_id(&identifiers[0].to_string());

            if table_id.is_some() && self.catalog.is_valid_column(table_id.unwrap(), &orig) {
                if field.is_some() {
                    return Err(CrustyError::ValidationError(format!(
                        "The field {} could refer to more than one table listed in the query",
                        orig
                    )));
                }
                let new_name = format!("{}.{}", table, orig);
                field = Some(FieldIdentifier::new_column_alias(table, &new_name, &orig));
            }
        }

        field.ok_or_else(|| {
            CrustyError::ValidationError(format!(
                "The field {} is not present in tables listed in the query",
                orig
            ))
        })
    }

    /// Translates a sqlparser::ast to a LogicalPlan.
    ///
    /// Validates the columns and tables referenced using the catalog.
    /// All table names referenced in from and join clauses are added to self.tables.
    ///
    /// # Arguments
    ///
    /// * `sql` - AST to transalte.
    /// * `catalog` - Catalog for validation.
    pub fn from_sql(sql: &sqlparser::ast::Query, catalog: &T) -> Result<LogicalPlan, CrustyError> {
        let mut translator = TranslateAndValidate::new(catalog);
        translator.process_query(sql)?;
        Ok(translator.plan)
    }

    /// Helper function to recursively process sqlparser::ast::Query
    ///
    /// # Arguments
    ///
    /// * `query` - AST to process.
    fn process_query(&mut self, query: &sqlparser::ast::Query) -> Result<(), CrustyError> {
        match &query.body {
            SetExpr::Select(b) => {
                let select = &*b;
                self.process_select(select)
            }
            SetExpr::Query(_) => {
                //TODO NOT HANDLED
                Err(CrustyError::ValidationError(String::from(
                    "Query ops not supported ",
                )))
            }
            SetExpr::SetOperation {
                op: _,
                all: _,
                left: _,
                right: _,
            } => {
                //TODO NOT HANDLED
                Err(CrustyError::ValidationError(String::from(
                    "Set operations not supported ",
                )))
            }
            SetExpr::Values(_) => {
                //TODO NOT HANDLED
                Err(CrustyError::ValidationError(String::from(
                    "Value operation not supported ",
                )))
            }
            SetExpr::Insert(_) => {
                //TODO NOT HANDLED
                Err(CrustyError::ValidationError(String::from(
                    "Insert operation not supported ",
                )))
            }
        }
    }

    /// Helper function to recursively process sqlparser::ast::Select
    ///
    /// # Arguments
    ///
    /// * `query` - AST of a select query to process.
    fn process_select(&mut self, select: &sqlparser::ast::Select) -> Result<(), CrustyError> {
        // Pointer to the current node.
        let mut node = None;

        // Distinct
        if select.distinct {
            //TODO NOT HANDLED
            return Err(CrustyError::ValidationError(String::from(
                "Distinct not supported ",
            )));
        }

        // Doesn't need the for loop rn but keeping for the future when cross products are supported.
        // From
        if select.from.len() > 1 {
            //TODO NOT HANDLED
            return Err(CrustyError::ValidationError(String::from(
                "Cross product not supported ",
            )));
        }
        for sel in &select.from {
            node = Some(self.process_table_factor(&sel.relation)?);
            // Join
            for join in &sel.joins {
                let join_node = self.process_join(join, node.unwrap())?;
                node = Some(join_node);
            }
        }

        // Where
        if let Some(expr) = &select.selection {
            let predicate = self.process_binary_op(expr)?;
            // table references in filter
            let table = match &predicate {
                Predicate::SimplePredicate(simple_predicate) => {
                    match (&simple_predicate.left, &simple_predicate.right) {
                        (PredExpr::Literal(_), PredExpr::Ident(id)) => id.table().to_string(),
                        (PredExpr::Ident(id), PredExpr::Literal(_)) => id.table().to_string(),
                        _ => {
                            return Err(CrustyError::ValidationError(String::from("Only where predicates with at least one identifier and at least one literal are supported")));
                        }
                    }
                }
                Predicate::CompoundPredicate(compound_predicates) => {
                    let simple_predicate = &compound_predicates.simple_predicates[0];
                    let table;
                    match (&simple_predicate.left, &simple_predicate.right) {
                        (PredExpr::Literal(_), PredExpr::Ident(id)) => table = id.table(),
                        (PredExpr::Ident(id), PredExpr::Literal(_)) => table = id.table(),
                        _ => {
                            return Err(CrustyError::ValidationError(String::from("Only where predicates with at least one indentifier and at least one literal are supported")));
                        }
                    }
                    for simple_predicate in &compound_predicates.simple_predicates {
                        let id = match (&simple_predicate.left, &simple_predicate.right) {
                            (PredExpr::Literal(_), PredExpr::Ident(id)) => id,
                            (PredExpr::Ident(id), PredExpr::Literal(_)) => id,
                            _ => {
                                return Err(CrustyError::ValidationError(String::from("Only where predicates with at least one indentifier and at least one literal are supported")));
                            }
                        };
                        if id.table() != table {
                            return Err(CrustyError::ValidationError(String::from(
                                "Where includes identifiers to columns in multiple tables",
                            )));
                        }
                    }
                    table.to_string()
                }
            };

            let op = FilterNode { table, predicate };
            let idx = self.plan.add_node(LogicalOp::Filter(op));
            self.plan.add_edge(idx, node.unwrap());
            node = Some(idx);
        }

        if select.having.is_some() {
            //TODO NOT HANDLED
            return Err(CrustyError::ValidationError(String::from(
                "Having not supported",
            )));
        }

        // Select
        let mut fields = Vec::new();
        let mut has_agg = false;
        let mut wildcard = false;
        for item in &select.projection {
            let field = match item {
                SelectItem::Wildcard => {
                    if select.projection.len() > 1 {
                        return Err(CrustyError::ValidationError(String::from(
                            "Cannot select wildcard and exp in same select",
                        )));
                    }
                    wildcard = true;
                    break;
                }
                SelectItem::UnnamedExpr(expr) => self.expr_to_ident(expr)?,
                SelectItem::ExprWithAlias { expr, alias } => {
                    let mut field = self.expr_to_ident(expr)?;
                    field.set_alias(alias.to_string());
                    field
                }
                _ => {
                    //TODO NOT HANDLED
                    return Err(CrustyError::ValidationError(String::from(
                        "Select unsupported expression",
                    )));
                }
            };
            if field.agg_op().is_some() {
                has_agg = true;
            }
            fields.push(field);
        }

        // Aggregates and group by
        if has_agg {
            let mut group_by = Vec::new();
            {
                let mut group_set = HashSet::new();
                for expr in &select.group_by {
                    let col = match expr {
                        Expr::Identifier(name) => name,
                        _ => {
                            return Err(CrustyError::ValidationError(String::from(
                                "Group by unsupported expression",
                            )));
                        }
                    };
                    let field = self.disambiguate_name(vec![&col.value])?;
                    group_set.insert(field.column().to_string());
                    group_by.push(field);
                }

                // Checks that only aggregates and group by fields are projected out
                for f in &fields {
                    if f.agg_op().is_none() && !group_set.contains(f.column()) {
                        return Err(CrustyError::ValidationError(format!(
                            "The expression '{}' must be part of an aggregate function or group by",
                            f.column()
                        )));
                    }
                }
            }
            let op = AggregateNode {
                fields: fields.clone(),
                group_by,
            };
            let idx = self.plan.add_node(LogicalOp::Aggregate(op));
            self.plan.add_edge(idx, node.unwrap());
            node = Some(idx);

            // Replace field column names with aliases to project
            fields = fields
                .iter()
                .map(|f| {
                    let name = f.alias().unwrap_or_else(|| f.column());
                    FieldIdentifier::new(f.table(), name)
                })
                .collect();
        }
        let identifiers = if wildcard {
            ProjectIdentifiers::Wildcard
        } else {
            ProjectIdentifiers::List(fields)
        };
        let op = ProjectNode { identifiers };
        let idx = self.plan.add_node(LogicalOp::Project(op));
        self.plan.add_edge(idx, node.unwrap());
        Ok(())
    }

    /// Creates a corresponding LogicalOp, adds it to self.plan, and returns the OpIndex.
    ///
    /// Helper function to process sqlparser::ast::TableFactor.
    ///
    /// # Arguments
    ///
    /// * `tf` - Table to process.
    fn process_table_factor(
        &mut self,
        tf: &sqlparser::ast::TableFactor,
    ) -> Result<OpIndex, CrustyError> {
        match tf {
            TableFactor::Table { name, .. } => {
                let name = get_name(name)?;
                let table_id = self
                    .catalog
                    .get_table_id(&name)
                    .ok_or_else(|| CrustyError::CrustyError("Missing Table".to_string()))?;
                if !self.catalog.is_valid_table(table_id) {
                    return Err(CrustyError::ValidationError(String::from(
                        "Invalid table name",
                    )));
                }
                self.tables.push(name.clone());
                let op = ScanNode {
                    alias: name,
                    container_id: table_id,
                };
                Ok(self.plan.add_node(LogicalOp::Scan(op)))
            }
            _ => Err(CrustyError::ValidationError(String::from(
                "Nested joins and derived tables not supported",
            ))),
        }
    }

    /// Returns the name of the table from the node, if the node is a table level operator, like scan. Otherwise, return none.
    ///
    /// # Arguments
    ///
    /// * `node` - Node to get the table name from.
    fn get_table_alias_from_op(&self, node: OpIndex) -> Option<String> {
        match &self.plan.get_operator(node)? {
            LogicalOp::Scan(ScanNode { alias, .. }) => Some(alias.clone()),
            _ => None,
        }
    }

    /// Parses sqlparser::ast::Join into a Join LogicalOp, adds the Op to
    /// logical plan, and returns OpIndex of the join node.
    ///
    /// # Arguments
    ///
    /// * `join` - The join node to parse.
    /// * `left_table_node` - Node containing the left table to join.
    fn process_join(
        &mut self,
        join: &sqlparser::ast::Join,
        left_table_node: OpIndex,
    ) -> Result<OpIndex, CrustyError> {
        let right_table_node = self.process_table_factor(&join.relation)?;
        let jc = match &join.join_operator {
            JoinOperator::Inner(jc) => jc,
            _ => {
                return Err(CrustyError::ValidationError(String::from(
                    "Unsupported join type",
                )));
            }
        };

        if let JoinConstraint::On(expr) = jc {
            let predicate = self.process_simple_predicate(expr)?;
            let left = predicate
                .left
                .ident()
                .ok_or_else(|| {
                    CrustyError::ValidationError(String::from("Invalid join predicate"))
                })?
                .clone();
            let right = predicate
                .right
                .ident()
                .ok_or_else(|| {
                    CrustyError::ValidationError(String::from("Invalid join predicate"))
                })?
                .clone();
            let op = JoinNode {
                left,
                right,
                op: predicate.op,
                left_table: self.get_table_alias_from_op(left_table_node),
                right_table: self.get_table_alias_from_op(right_table_node),
            };
            let idx = self.plan.add_node(LogicalOp::Join(op));
            self.plan.add_edge(idx, right_table_node);
            self.plan.add_edge(idx, left_table_node);
            return Ok(idx);
        }
        Err(CrustyError::ValidationError(String::from(
            "Unsupported join type",
        )))
    }
    /// Parses an expression to a predicate node.
    ///
    /// # Arguments
    ///
    /// * `expr` - Expression to parse.
    fn process_binary_op(&self, expr: &Expr) -> Result<Predicate, CrustyError> {
        match expr {
            Expr::BinaryOp { op, .. } => match Self::binary_op_to_predicate_op(op)? {
                PredicateOp::SimplePredicateOp(_) => Ok(Predicate::SimplePredicate(
                    self.process_simple_predicate(expr)?,
                )),
                PredicateOp::CompoundPredicateOp(_) => Ok(Predicate::CompoundPredicate(
                    self.process_compound_predicate(expr)?,
                )),
            },
            _ => Err(CrustyError::ValidationError(String::from(
                "Expected binary operation",
            ))),
        }
    }

    /// Parses an expression to a simple predicate.
    ///
    /// # Arguments
    ///
    /// * `expr` - Expression to parse.
    fn process_simple_predicate(&self, expr: &Expr) -> Result<SimplePredicate, CrustyError> {
        match expr {
            Expr::BinaryOp { left, op, right } => Ok(SimplePredicate {
                left: self.expr_to_pred_expr(left)?,
                right: self.expr_to_pred_expr(right)?,
                op: Self::binary_op_to_simple_predicate_op(op)?,
            }),
            _ => Err(CrustyError::ValidationError(String::from(
                "Expected binary operation",
            ))),
        }
    }

    /// Parses an expression to a compound predicate.
    ///
    /// # Arguments
    ///
    /// * `expr` - Expression to parse.
    fn process_compound_predicate(&self, expr: &Expr) -> Result<CompoundPredicate, CrustyError> {
        match expr {
            Expr::BinaryOp { op, .. } => {
                let compound_predicate_op = Self::binary_op_to_compound_predicate_op(op)?;
                let simple_predicates =
                    self.process_compound_predicate_helper(expr, compound_predicate_op.clone())?;
                Ok(CompoundPredicate {
                    op: compound_predicate_op,
                    simple_predicates,
                })
            }
            _ => Err(CrustyError::ValidationError(String::from(
                "Expected binary operation",
            ))),
        }
    }

    /// Recursive helper function for parsing compound predicates.
    ///
    /// # Arguments
    ///
    /// * `expr` - Expression to parse
    /// * `test_compound_op` - the compound operator that has been used so far in the parsing. If expr is a compound predicate, it must use the same compount operator.
    fn process_compound_predicate_helper(
        &self,
        expr: &Expr,
        test_compound_op: CompoundPredicateOp,
    ) -> Result<Vec<SimplePredicate>, CrustyError> {
        match expr {
            Expr::BinaryOp { left, op, right } => match Self::binary_op_to_predicate_op(op)? {
                PredicateOp::SimplePredicateOp(_) => Ok(vec![self.process_simple_predicate(expr)?]),
                PredicateOp::CompoundPredicateOp(compound_op) => {
                    if compound_op == test_compound_op {
                        let mut res =
                            self.process_compound_predicate_helper(left, test_compound_op.clone())?;
                        res.append(
                            &mut self.process_compound_predicate_helper(right, test_compound_op)?,
                        );

                        Ok(res)
                    } else {
                        Err(CrustyError::ValidationError(String::from(
                            "Cannot make a compound predicate with ors and ands",
                        )))
                    }
                }
            },
            _ => Err(CrustyError::ValidationError(String::from(
                "Expected binary operation",
            ))),
        }
    }

    /// Parses the non-operator parts of the expression to predicate expressions.
    ///
    /// # Arguments
    ///
    /// * `expr` - Non-operator part of the expression to parse.
    fn expr_to_pred_expr(&self, expr: &Expr) -> Result<PredExpr, CrustyError> {
        match expr {
            Expr::Value(val) => match val {
                Value::Number(s, _) => {
                    let i = s.parse::<i32>().map_err(|_| {
                        CrustyError::ValidationError(format!("Unsupported literal {}", s))
                    })?;
                    let f = Field::IntField(i);
                    Ok(PredExpr::Literal(f))
                }
                Value::SingleQuotedString(s) => {
                    let f = Field::StringField(s.to_string());
                    Ok(PredExpr::Literal(f))
                }
                _ => Err(CrustyError::ValidationError(String::from(
                    "Unsupported literal in predicate",
                ))),
            },
            _ => Ok(PredExpr::Ident(self.expr_to_ident(expr)?)),
        }
    }

    /// Parses binary operator to predicate operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Binary operator to parse.
    fn binary_op_to_predicate_op(op: &BinaryOperator) -> Result<PredicateOp, CrustyError> {
        match op {
            BinaryOperator::Gt => Ok(PredicateOp::SimplePredicateOp(
                SimplePredicateOp::GreaterThan,
            )),
            BinaryOperator::Lt => Ok(PredicateOp::SimplePredicateOp(SimplePredicateOp::LessThan)),
            BinaryOperator::GtEq => Ok(PredicateOp::SimplePredicateOp(
                SimplePredicateOp::GreaterThanOrEq,
            )),
            BinaryOperator::LtEq => Ok(PredicateOp::SimplePredicateOp(
                SimplePredicateOp::LessThanOrEq,
            )),
            BinaryOperator::Eq => Ok(PredicateOp::SimplePredicateOp(SimplePredicateOp::Equals)),
            BinaryOperator::NotEq => Ok(PredicateOp::SimplePredicateOp(SimplePredicateOp::NotEq)),

            BinaryOperator::And => Ok(PredicateOp::CompoundPredicateOp(CompoundPredicateOp::And)),
            BinaryOperator::Or => Ok(PredicateOp::CompoundPredicateOp(CompoundPredicateOp::Or)),

            _ => Err(CrustyError::ValidationError(format!(
                "Expected predicate op, got {}",
                op,
            ))),
        }
    }

    /// Parses binary operator to simple predicate operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Binary operator to parse.
    fn binary_op_to_simple_predicate_op(
        op: &BinaryOperator,
    ) -> Result<SimplePredicateOp, CrustyError> {
        match op {
            BinaryOperator::Gt => Ok(SimplePredicateOp::GreaterThan),
            BinaryOperator::Lt => Ok(SimplePredicateOp::LessThan),
            BinaryOperator::GtEq => Ok(SimplePredicateOp::GreaterThanOrEq),
            BinaryOperator::LtEq => Ok(SimplePredicateOp::LessThanOrEq),
            BinaryOperator::Eq => Ok(SimplePredicateOp::Equals),
            BinaryOperator::NotEq => Ok(SimplePredicateOp::NotEq),
            _ => Err(CrustyError::ValidationError(format!(
                "Expected simple predicate op, got {}",
                op,
            ))),
        }
    }

    /// Parses binary operator to compound predicate operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Binary operator to parse.
    fn binary_op_to_compound_predicate_op(
        op: &BinaryOperator,
    ) -> Result<CompoundPredicateOp, CrustyError> {
        match op {
            BinaryOperator::And => Ok(CompoundPredicateOp::And),
            BinaryOperator::Or => Ok(CompoundPredicateOp::Or),
            _ => Err(CrustyError::ValidationError(format!(
                "Expected compound predicate op, got {}",
                op,
            ))),
        }
    }

    /// Validates that an aggregate operation is valid for the type of field.
    ///
    /// Field must
    /// * be disambiguated so that field.column() returns a str of the form table.column
    /// * have an associated op
    ///
    /// # Arguments
    ///
    /// * `field` - Field to be aggregated.
    fn validate_aggregate(&self, field: &FieldIdentifier) -> Result<(), CrustyError> {
        let split_field: Vec<&str> = field.column().split('.').collect();
        if field.agg_op().is_none() || split_field.len() != 2 {
            return Ok(());
        }
        let table_name = field.table();
        let col_name = split_field[1];
        let alias = field.alias().unwrap_or_else(|| field.column());
        let op = field.agg_op().unwrap();
        let table_id = self
            .catalog
            .get_table_id(&table_name.to_string())
            .ok_or_else(|| CrustyError::CrustyError("Missing Table".to_string()))?;

        let schema = self.catalog.get_table_schema(table_id)?;
        let attr = schema
            .get_attribute(*schema.get_field_index(col_name).unwrap())
            .unwrap();

        match attr.dtype() {
            DataType::Int => Ok(()),
            DataType::String => match op {
                AggOp::Count | AggOp::Max | AggOp::Min => Ok(()),
                _ => Err(CrustyError::ValidationError(format!(
                    "Cannot perform operation {} on field {}",
                    op, alias,
                ))),
            },
        }
    }

    /// Converts a sqparser::ast::Expr to a LogicalOp::FieldIdent.
    ///
    /// # Arguments
    ///
    /// * `expr` - Expression to be converted.
    fn expr_to_ident(&self, expr: &Expr) -> Result<FieldIdentifier, CrustyError> {
        match expr {
            Expr::Identifier(name) => self.disambiguate_name(vec![&name.value]),
            Expr::CompoundIdentifier(names) => {
                self.disambiguate_name(names.iter().map(|s| s.value.as_ref()).collect())
            }
            Expr::Function(Function { name, args, .. }) => {
                let op = match &get_name(name)?.to_uppercase()[..] {
                    "AVG" => AggOp::Avg,
                    "COUNT" => AggOp::Count,
                    "MAX" => AggOp::Max,
                    "MIN" => AggOp::Min,
                    "SUM" => AggOp::Sum,
                    _ => {
                        return Err(CrustyError::ValidationError(String::from(
                            "Unsupported SQL function",
                        )));
                    }
                };
                if args.is_empty() || args.len() > 1 {
                    return Err(CrustyError::ValidationError(format!(
                        "Wrong number of args in {} operation",
                        name
                    )));
                }
                let arg = match &args[0] {
                    FunctionArg::Named { name: _, arg } => arg,
                    FunctionArg::Unnamed(arg) => arg,
                };
                let mut field = match arg {
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_) => self.expr_to_ident(arg)?,
                    _ => {
                        return Err(CrustyError::ValidationError(String::from(
                            "Aggregate over unsupported expression",
                        )));
                    }
                };
                field.set_op(op);
                field.default_alias();
                self.validate_aggregate(&field)?;
                Ok(field)
            }
            _ => Err(CrustyError::ValidationError(String::from(
                "Unsupported expression",
            ))),
        }
    }
}

/* FIXME
#[cfg(test)]
mod test {
    use super::super::test::*;
    use super::*;

    mod translate {
        use super::*;

        #[test]
        fn test_from_sql_project() {
            let sql = "Select * From Table;";
            let ast = get_select_ast(sql);
            let db = MockDb {};
            let lp = TranslateAndValidate::from_sql(&ast, &db).unwrap();
            assert_eq!(lp.node_count(), 2);
            assert_eq!(lp.edge_count(), 1);
            match lp.get_operator(lp.root().unwrap()) {
                Some(LogicalOp::Project(_)) => (),
                _ => panic!("Incorrect root"),
            }
        }

        #[test]
        #[should_panic]
        fn test_missing_group_by() {
            let sql = format!(
                "Select {}, count({}) from {}",
                B_COLS[1], B_COLS[0], TABLE_B,
            );
            let ast = get_select_ast(&sql);
            let db = MockDb {};
            TranslateAndValidate::from_sql(&ast, &db).unwrap();
        }
    }

    mod validate {
        use super::*;

        #[test]
        fn test_disambiguate_name() -> Result<(), CrustyError> {
            let db = test_db();
            let mut tv = TranslateAndValidate::new(&db);
            tv.tables = vec![
                TABLE_A.to_string(),
                TABLE_B.to_string(),
                TABLE_C.to_string(),
            ];

            // Valid fields
            let full_name = format!("{}.{}", TABLE_B, B_COLS[0]);
            let f1 = tv.disambiguate_name(vec![B_COLS[0]])?;
            assert_eq!(&full_name, f1.column());
            assert_eq!(B_COLS[0], f1.alias().unwrap());

            let f2 = tv.disambiguate_name(vec![TABLE_B, B_COLS[0]])?;
            assert_eq!(&full_name, f2.column());

            // Mismatched table and field
            let res = tv.disambiguate_name(vec![TABLE_A, B_COLS[0]]);
            assert!(res.is_err());

            // Non-existent table
            let res = tv.disambiguate_name(vec!["_table_", B_COLS[0]]);
            assert!(res.is_err());

            // Non-existent field
            let res = tv.disambiguate_name(vec!["_field_"]);
            assert!(res.is_err());

            // Ambiguous field
            let res = tv.disambiguate_name(vec![A_COLS[0]]);
            assert!(res.is_err());

            Ok(())
        }

        #[test]
        fn test_validate_table_name() {
            let db = test_db();

            // Valid table
            let sql = format!("Select * from {}", TABLE_B);
            let ast = get_select_ast(&sql);
            let res = TranslateAndValidate::from_sql(&ast, &db);
            assert!(res.is_ok());

            // Invalid table
            let ast = get_select_ast("Select * from fake_table");
            let res = TranslateAndValidate::from_sql(&ast, &db);
            assert!(res.is_err());
        }

        #[test]
        fn test_validate_column() {
            let db = test_db();

            // Valid column
            let sql = format!("Select {} from {}", B_COLS[0], TABLE_B);
            let ast = get_select_ast(&sql);
            let res = TranslateAndValidate::from_sql(&ast, &db);
            assert!(res.is_ok());

            // Invalid column
            let sql = format!("Select {} from {}", A_COLS[0], TABLE_B);
            let ast = get_select_ast(&sql);
            let res = TranslateAndValidate::from_sql(&ast, &db);
            assert!(res.is_err());
        }

        #[test]
        fn test_validate_ambiguous_column() {
            let db = test_db();

            // Ambigous column
            let sql = format!(
                "Select {} from {ta} Join {tc} on {ta}.{} = {tc}.{}",
                A_COLS[0],
                A_COLS[0],
                C_COLS[0],
                ta = TABLE_A,
                tc = TABLE_C
            );
            let ast = get_select_ast(&sql);
            let res = TranslateAndValidate::from_sql(&ast, &db);
            assert!(res.is_err());

            // Ambigous join on
            let sql = format!(
                "Select * from {ta} Join {tc} on {} = {}",
                A_COLS[0],
                C_COLS[0],
                ta = TABLE_A,
                tc = TABLE_C
            );
            let ast = get_select_ast(&sql);
            let res = TranslateAndValidate::from_sql(&ast, &db);
            assert!(res.is_err());

            // Unambigous column / join
            let sql = format!(
                "Select {ta}.{} from {ta} Join {tc} on {ta}.{} = {tc}.{}",
                A_COLS[0],
                A_COLS[0],
                C_COLS[0],
                ta = TABLE_A,
                tc = TABLE_C
            );
            let ast = get_select_ast(&sql);
            let res = TranslateAndValidate::from_sql(&ast, &db);
            assert!(res.is_ok());
        }
    }
}
*/
