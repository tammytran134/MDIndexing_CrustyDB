use sqlparser::ast::Statement;
use std::sync::Arc;

use crate::queryexe::query::TranslateAndValidate;
use common::ids::LogicalTimeStamp;
use common::physical_plan::PhysicalPlan;
use common::{get_name, testutil, CrustyError, QueryResult};
use optimizer::optimizer::Optimizer;
use txn_manager::transactions::Transaction;

use crate::csv_utils;
use crate::database_state::DatabaseState;
use crate::server_state::ServerState;
use crate::sql_parser::{ParserResponse, SQLParser};
use crate::worker::Message;
use crate::Executor;
use common::commands;
use std::fs::OpenOptions;
use std::io::Write;

pub struct Conductor {
    pub parser: SQLParser,
    pub optimizer: Optimizer,
    pub executor: Executor,
}

impl Conductor {
    pub fn new(
        parser: SQLParser,
        optimizer: Optimizer,
        executor: Executor,
    ) -> Result<Self, CrustyError> {
        let conductor = Conductor {
            parser,
            optimizer,
            executor,
        };
        Ok(conductor)
    }

    /// Processes command entered by the user.
    ///
    /// # Arguments
    ///
    /// * `cmd` - Command to execute.
    /// * `client_id` - id of client running command.
    /// * `server_state` the shared ref to server
    pub fn run_command(
        &mut self,
        command: commands::Commands,
        client_id: u64,
        server_state: &'static ServerState,
    ) -> Result<String, CrustyError> {
        match command {
            commands::Commands::Create(name) => {
                info!("Processing COMMAND::Create {:?}", name);
                server_state.create_database(name)
            }
            commands::Commands::Connect(name) => {
                // Check exists and load.
                // TODO: Figure out about using &str.
                info!("Processing COMMAND::Connect {:?}", name);
                server_state.connect_to_db(name, client_id)
            }
            commands::Commands::Import(path_and_name) => {
                info!("Processing COMMAND::Import {:?}", path_and_name);
                server_state.import_database(path_and_name, client_id)
            }
            commands::Commands::RegisterQuery(name_and_plan_path) => {
                // Register a query (as a physical plan) to be executed at a later time (a stored procedure or view)
                info!("Processing COMMAND::RegisterQuery {:?}", name_and_plan_path);
                server_state.register_query(name_and_plan_path, client_id)
            }
            commands::Commands::RunQueryFull(args) => {
                // Execute a query  that has been registered before
                info!("Processing COMMAND::RunQueryFull {:?}", args);

                // Parse arguments
                let mut tokens = args.split_whitespace();
                let possible_query_name = tokens.next();
                let possible_cache = tokens.next();
                let possible_timestamp = tokens.next();
                if tokens.next().is_some() {
                    return Err(CrustyError::CrustyError("Too many arguments".to_string()));
                } else if possible_query_name.is_none()
                    || possible_cache.is_none()
                    || possible_timestamp.is_none()
                {
                    return Err(CrustyError::CrustyError(format!(
                        "Missing arguments \"{}\"",
                        args
                    )));
                }
                let query_name = possible_query_name.unwrap();
                let _cache: bool = match possible_cache.unwrap().parse() {
                    Ok(v) => v,
                    Err(e) => return Err(CrustyError::CrustyError(format!("Bad cache: {}", e))),
                };
                let timestamp: LogicalTimeStamp = match possible_timestamp.unwrap().parse() {
                    Ok(ts) => ts,
                    Err(e) => {
                        return Err(CrustyError::CrustyError(format!("Bad timestamp: {}", e)))
                    }
                };

                // Get db id.
                let db_id_ref = server_state.active_connections.read().unwrap();
                let db_state = match db_id_ref.get(&client_id) {
                    Some(db_id) => {
                        let db_ref = server_state.id_to_db.read().unwrap();
                        *db_ref.get(db_id).unwrap()
                    }
                    None => {
                        return Err(CrustyError::CrustyError(String::from(
                            "No active DB or DB not found",
                        )))
                    }
                };

                // Get query plan.
                let query_plan =
                    server_state.begin_query(query_name, None, timestamp, client_id)?;

                // Run query.
                self.run_query(query_plan, db_state, timestamp)?;

                // Update metadata after finishing query.
                server_state.finish_query(query_name, client_id)?;

                Ok(format!("Finished running query \"{}\"", query_name))
            }
            #[allow(unused_variables)]
            commands::Commands::RunQueryPartial(name_and_range) => todo!(),
            commands::Commands::ConvertQuery(args) => {
                // Convert a SQL statement/query into a physical plan. Used for registering queries.

                info!("Processing COMMAND::ConvertQuery {:?}", args);
                let mut tokens = args.split('|');
                let json_file_name = tokens.next();
                let sql = tokens.next();

                if json_file_name.is_none() || sql.is_none() {
                    return Err(CrustyError::CrustyError(format!(
                        "Missing arguments should be jsonfile|sql \"{}\"",
                        args
                    )));
                }
                info!("JSON {} SQL {}", json_file_name.unwrap(), sql.unwrap());
                let file_name: String = json_file_name.unwrap().split_whitespace().collect();

                if let ParserResponse::SQL(statements) =
                    SQLParser::parse_sql(sql.unwrap().to_string())
                {
                    if statements.len() != 1 {
                        return Err(CrustyError::CrustyError(format!(
                            "Can only store single SQL statement. Got {}",
                            statements.len()
                        )));
                    }

                    let db_id_ref = server_state.active_connections.read().unwrap();
                    let db_state = match db_id_ref.get(&client_id) {
                        Some(db_id) => {
                            let db_ref = server_state.id_to_db.read().unwrap();
                            *db_ref.get(db_id).unwrap()
                        }
                        None => {
                            return Err(CrustyError::CrustyError(String::from(
                                "No active DB or DB not found",
                            )))
                        }
                    };

                    let statement = statements.get(0).unwrap();
                    if let Statement::Query(query) = statement {
                        let db = &db_state.database;
                        debug!("Obtaining Logical Plan from query's AST");
                        let logical_plan = TranslateAndValidate::from_sql(query, db)?;

                        debug!("Converting this Logical Plan to a Physical Plan");
                        let physical_plan =
                            self.optimizer
                                .logical_plan_to_physical_plan(logical_plan, db, true)?;

                        let plan_json = physical_plan.to_json();
                        let x = serde_json::to_vec(&plan_json).unwrap();

                        let file = OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(file_name);
                        match file {
                            Ok(mut file) => match file.write(&x) {
                                Ok(_size) => Ok("ok".to_string()),
                                Err(e) => Err(CrustyError::IOError(format!("{:?}", e))),
                            },
                            Err(e) => Err(CrustyError::CrustyError(e.to_string())),
                        }
                    } else {
                        Err(CrustyError::CrustyError(String::from(
                            "SQL statement is not a query.",
                        )))
                    }
                } else {
                    Err(CrustyError::CrustyError(String::from(
                        "Can only store valid SQL statement.",
                    )))
                }
            }
            commands::Commands::ShowTables => {
                info!("Processing COMMAND::ShowTables");
                let db_id_ref = server_state.active_connections.read().unwrap();
                match db_id_ref.get(&client_id) {
                    Some(db_id) => {
                        let db_ref = server_state.id_to_db.read().unwrap();
                        let db_state = db_ref.get(db_id).unwrap();

                        let table_names = db_state.get_table_names()?;
                        Ok(table_names)
                    }
                    None => Ok(String::from("No active DB or DB not found")),
                }
            }
            commands::Commands::ShowQueries => {
                info!("Processing COMMAND::ShowQueries");
                let db_id_ref = server_state.active_connections.read().unwrap();
                match db_id_ref.get(&client_id) {
                    Some(db_id) => {
                        let db_ref = server_state.id_to_db.read().unwrap();
                        let db_state = db_ref.get(db_id).unwrap();

                        let registered_query_names = db_state.get_registered_query_names()?;
                        Ok(registered_query_names)
                    }
                    None => Ok(String::from("No active DB or DB not found")),
                }
            }
            commands::Commands::ShowDatabases => {
                info!("Processing COMMAND::ShowDatabases");
                let id_map = server_state.id_to_db.read();
                let mut names: Vec<String> = Vec::new();
                match id_map {
                    Ok(map) => {
                        for (id, db) in &*map {
                            debug!(" id {}", id);
                            names.push(db.name.clone());
                        }
                    }
                    _ => panic!("Failed to get lock"),
                }
                if names.is_empty() {
                    Ok(String::from("No databases found"))
                } else {
                    Ok(names.join(","))
                }
            }
            commands::Commands::Reset => {
                info!("Processing COMMAND::Reset");
                server_state.reset_database()?;
                Ok(String::from("Reset all of the database"))
            }
            commands::Commands::Generate(args) => {
                info!("Processing COMMAND::Generate {:?}", args);
                // Parse arguments
                let mut tokens = args.split_whitespace();
                let possible_csv_filename = tokens.next();
                let possible_n = tokens.next();
                if tokens.next().is_some() {
                    return Err(CrustyError::CrustyError("Too many arguments".to_string()));
                } else if possible_csv_filename.is_none() || possible_n.is_none() {
                    return Err(CrustyError::CrustyError(format!(
                        "Missing arguments \"{}\"",
                        args
                    )));
                }
                let csv_file_name = possible_csv_filename.unwrap();
                let n: u64 = match possible_n.unwrap().parse() {
                    Ok(v) => v,
                    Err(e) => return Err(CrustyError::CrustyError(format!("Bad n: {}", e))),
                };
                let tuples = testutil::gen_test_tuples(n);
                csv_utils::write_tuples_to_new_csv(csv_file_name.to_string(), tuples)
            }
            commands::Commands::Test => {
                let queue = server_state.task_queue.lock().unwrap();
                queue.send(Message::Test).unwrap();
                Ok(String::from("Test OK"))
            }
            commands::Commands::ExecuteSQL(_sql) => {
                panic!("Should never get here");
            }
            commands::Commands::Shutdown => {
                panic!("Received a shutdown. Never should have made it this far.");
            }
            commands::Commands::CloseConnection => {
                panic!("Received a close. Never should have made it this far.");
            }
            commands::Commands::QuietMode => {
                panic!("should not get here with quiet mode");
            }
        }
    }

    /// Runs SQL commands depending on the first statement.
    ///
    /// # Arguments
    ///
    /// * `cmd` - Tokenized command into statements.
    /// * `id` - Thread id for lock management.
    #[allow(unused_variables)]
    pub fn run_sql(
        &mut self,
        cmd: Vec<Statement>,
        db_state: &'static DatabaseState,
    ) -> Result<common::QueryResult, CrustyError> {
        if cmd.is_empty() {
            Err(CrustyError::CrustyError(String::from("Empty SQL command")))
        } else {
            match cmd.first().unwrap() {
                Statement::CreateTable {
                    name: table_name,
                    columns,
                    constraints,
                    ..
                } => {
                    info!("Processing CREATE table: {:?}", table_name);
                    db_state.create_table(&get_name(table_name)?, columns, constraints)
                }
                Statement::Query(qbox) => {
                    debug!("Processing SQL Query");
                    let db = &db_state.database;

                    // After optimizer has done its job, we obtain a physical representation of this logical-plan
                    // This physical representation depends on the Executor implementation, so Executors must
                    // provide a function that takes a logical plan, catalog, storage manager, etc, and gives
                    // back a physical plan which is a thing that the Executor knows how to interpret

                    debug!("Obtaining Logical Plan from query's AST");
                    let logical_plan = TranslateAndValidate::from_sql(qbox, db)?;

                    debug!("Converting this Logical Plan to a Physical Plan");
                    let physical_plan =
                        self.optimizer
                            .logical_plan_to_physical_plan(logical_plan, db, false)?;
                    debug!("physical plan {:?}", physical_plan);
                    self.run_query(
                        Arc::new(physical_plan),
                        db_state,
                        db_state.get_current_time(),
                    )
                }
                Statement::Insert {
                    table_name,
                    columns,
                    source,
                    ..
                } => Err(CrustyError::CrustyError(String::from(
                    "Inserts not currently supported",
                ))),
                _ => Err(CrustyError::CrustyError(String::from("Not supported"))),
            }
        }
    }

    /// Runs a given query.
    ///
    /// # Arguments
    ///
    /// * `query` - Query to run.
    /// * `id` - Thread id for lock management.
    #[allow(unused_variables)]
    fn run_query(
        &mut self,
        physical_plan: Arc<PhysicalPlan>,
        db_state: &'static DatabaseState,
        timestamp: LogicalTimeStamp,
    ) -> Result<QueryResult, CrustyError> {
        let db = &db_state.database;

        // Start transaction
        let txn = Transaction::new();

        debug!("Configuring Storage Manager");
        let op_iterator = Executor::physical_plan_to_op_iterator(
            db_state.storage_manager,
            db,
            &physical_plan,
            txn.tid(),
            timestamp,
        )?;
        // We populate the executor with the state: physical plan, and storage manager ref
        debug!("Configuring Physical Plan");
        self.executor.configure_query(op_iterator);

        // Finally, execute the query
        debug!("Executing query");
        let res = self.executor.execute();
        match res {
            Ok(qr) => Ok(qr),
            Err(e) => Err(e),
        }
    }
}
