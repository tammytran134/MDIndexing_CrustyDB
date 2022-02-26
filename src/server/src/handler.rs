use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

use crate::conductor::Conductor;
use crate::server_state::ServerState;
use crate::sql_parser::{ParserResponse, SQLParser};

use crate::Executor;
use common::commands::{Commands, Response};
use optimizer::optimizer::Optimizer;

/// Waits for user commands and dispatches the commands.
///
/// # Arguments
///
/// * `stream` - TCP stream containing user inputs.
pub fn handle_client_request(mut stream: TcpStream, server_state: &'static ServerState) {
    // FIXME: right now, this is unused
    let parser = SQLParser::new();
    let executor = Executor::new_ref(server_state.storage_manager);
    let optimizer = Optimizer::new();
    let mut conductor = Conductor::new(parser, optimizer, executor).unwrap();

    // FIXME: id is hash(incoming-ip), make this right
    // TODO: create a session for this client
    error!("TODO create session for client properly");
    let peer_ip_string = stream.peer_addr().unwrap().ip().to_string();
    let mut s = DefaultHasher::new();
    peer_ip_string.hash(&mut s);
    let client_id = s.finish();

    let mut quiet = false;

    let mut buffer = [0; 1024];
    //let bytes_read = stream.read(&mut buffer).unwrap();
    while match stream.read(&mut buffer) {
        Ok(size) => {
            if size == 0 {
                info!("Request is size 0 closing connection");
                server_state.close_client_connection(client_id);
                false
            } else {
                //println!("{:?}", buffer);
                let request_command: Commands = match serde_cbor::from_slice(&buffer[0..size]) {
                    Ok(command) => command,
                    Err(e) => {
                        println!("{} {:?}", e, buffer);
                        panic!("FIXME j");
                    }
                };
                debug!("Received request command {:?}", request_command);

                //TODO: Better way to handle client end?
                // FIXME: and close connection should be just another command
                let response: Response = match request_command {
                    Commands::Shutdown => {
                        stream
                            .write_all(&serde_cbor::to_vec(&Response::Shutdown).unwrap())
                            .unwrap();
                        stream.shutdown(Shutdown::Both).unwrap();
                        server_state.shutdown().unwrap();
                        std::process::exit(1);
                    }
                    Commands::QuietMode => {
                        info!("Going to QuietMode");
                        quiet = true;
                        Response::QuietOk
                    }
                    Commands::ExecuteSQL(sql) => {
                        match SQLParser::parse_sql(sql) {
                            // SQL Query
                            ParserResponse::SQL(ast) => {
                                let db_id_ref = server_state.active_connections.read().unwrap();
                                match db_id_ref.get(&client_id) {
                                    Some(db_id) => {
                                        let db_ref = server_state.id_to_db.read().unwrap();
                                        let db_state = db_ref.get(db_id).unwrap();
                                        match conductor.run_sql(ast, db_state) {
                                            Ok(qr) => {
                                                if quiet {
                                                    debug!("Query result is good. Sending QuietOK");
                                                    Response::QuietOk
                                                } else {
                                                    info!("Success running SQL query");
                                                    Response::QueryResult(qr)
                                                }
                                            }
                                            Err(err) => {
                                                info!("Error while executing SQL query");
                                                Response::Err(err.to_string())
                                            }
                                        }
                                    }
                                    None => {
                                        Response::Err("No active DB or DB not found".to_string())
                                    }
                                }
                            }
                            // Errors
                            ParserResponse::SQLError(e) => {
                                Response::Err(format!("SQL error: {}", e))
                            }
                            ParserResponse::SQLConstraintError(msg) => Response::Err(format!(
                                "Constraint error with your \
                                SQL statement: {}",
                                msg
                            )),
                            ParserResponse::Err => Response::Err("Unknown command".to_string()),
                        }
                    }
                    _ => match conductor.run_command(request_command, client_id, server_state) {
                        Ok(qr) => {
                            info!("Success COMMAND {:?}", qr);
                            Response::Msg(qr.to_string())
                        }
                        Err(err) => {
                            info!("Error while executing COMMAND error: {:?}", err);
                            Response::Err(err.to_string())
                        }
                    },
                };

                if quiet {
                    if let Response::Err(_) = response {
                        stream
                            .write_all(&serde_cbor::to_vec(&Response::QuietErr).unwrap())
                            .unwrap();
                    } else {
                        stream
                            .write_all(&serde_cbor::to_vec(&Response::QuietOk).unwrap())
                            .unwrap();
                    }
                } else {
                    stream
                        .write_all(&serde_cbor::to_vec(&response).unwrap())
                        .unwrap();
                }
                true
            }
        }
        Err(_) => {
            error!(
                "An error occurred, terminating connection with {}",
                stream.peer_addr().unwrap()
            );
            stream.shutdown(Shutdown::Both).unwrap();
            // FIXME: (raul) shut this down properly
            error!("Shutting down crustydb due to error...");
            std::process::exit(0);
        }
    } {}
}
