#[macro_use]
extern crate log;
use env_logger::Env;
extern crate clap;
use clap::{App, Arg};
#[macro_use]
extern crate serde;

use std::fs;
use std::net::TcpListener;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use crate::daemon::Daemon;
use crate::server_state::ServerState;
use crate::worker::Message;

mod conductor;
mod csv_utils;
mod daemon;
mod database_state;
mod handler;
mod query_registrar;
mod server_state;
mod sql_parser;
mod worker;

/// Re-export Storage manager here for this crate to use. This allows us to change
/// the storage manager by changing one use statement.
pub use common::storage_trait::StorageTrait;
// pub use memstore::storage_manager::StorageManager;
pub use heapstore::storage_manager::StorageManager;
pub use queryexe;
pub use queryexe::query::Executor;

// For delta based system
//pub use deltastore::storage_manager::DeltaStorageManager as StorageManager;
//pub use common::delta_storage_trait::DeltaStorageManagerTrait as StorageTrait;

#[derive(Deserialize, Debug)]
struct ServerConfig {
    host: String,
    port: String,
    db_path: String,
    hf_path: String,
    workers: usize,
}

/// Entry point for server.
///
/// Waits for user connections and creates a new thread for each connection.
fn main() {
    // Configure log environment
    env_logger::from_env(Env::default().default_filter_or("warning")).init();

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("host")
                .default_value("127.0.0.1")
                .help("Server IP address")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("port")
                .default_value("3333")
                .help("Server port number")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("db_path")
                .short("db")
                .long("db_path")
                .value_name("db_path")
                .default_value("persist/db/")
                .help("Path where DB is stored")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("hf_path")
                .long("hf_path")
                .value_name("hf_path")
                .default_value("persist/table/")
                .help("????")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("workers")
                .short("w")
                .long("workers")
                .value_name("workers")
                .default_value("2")
                .help("Number of worker threads")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("daemon")
                .long("daemon")
                .default_value("60")
                .help("Number of seconds for daemon thread to sleep between work")
                .takes_value(true),
        )
        .get_matches();

    let config = if let Some(c) = matches.value_of("config") {
        let config_path = c;
        let contents = fs::read_to_string(config_path).unwrap();
        serde_json::from_str(&contents).unwrap()
    } else {
        let host = matches.value_of("host").unwrap();
        let port = matches.value_of("port").unwrap();
        let db_path = matches.value_of("db_path").unwrap();
        let hf_path = matches.value_of("hf_path").unwrap();
        let workers = matches
            .value_of("workers")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        ServerConfig {
            host: host.to_string(),
            port: port.to_string(),
            db_path: db_path.to_string(),
            hf_path: hf_path.to_string(),
            workers,
        }
    };

    let daemon_seconds = matches.value_of("daemon").unwrap().parse::<u64>().unwrap();

    info!("Starting crustydb... {:?}", config);

    // Create channel / queue for jobs
    let (sender, receiver) = mpsc::channel::<Message>();
    // Receiver is shared by workers
    let receiver = Arc::new(Mutex::new(receiver));

    let server_state_box =
        Box::new(ServerState::new(config.db_path, config.hf_path, sender).unwrap());
    let server_state: &'static ServerState = Box::leak(server_state_box);
    //Create daemon thread
    let mut _daemon_thread = Daemon::new(server_state, daemon_seconds);

    //Create a worker pool and start it.
    let mut workers = Vec::with_capacity(config.workers);

    for id in 0..config.workers {
        workers.push(worker::Worker::new(id, Arc::clone(&receiver), server_state));
    }

    server_state.add_workers(workers);

    //Start listening to requests by spawning a handler per request.
    let mut bind_addr = config.host.clone();
    bind_addr.push(':');
    bind_addr.push_str(&config.port);
    let listener = TcpListener::bind(bind_addr).unwrap();

    // Accept connections and process them on independent threads.
    info!(
        "Server listening on with host {} on port {}",
        config.host, config.port
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Going to check
                debug!("New connection: {}", stream.peer_addr().unwrap());

                let _handler = thread::spawn(move || {
                    // Connection succeeded.
                    handler::handle_client_request(stream, server_state);
                });
            }
            Err(e) => {
                // Connection failed.
                error!("Error: {}", e);
            }
        }
    }
    //TODO look for signal capture for clean shutdown
    info!("shutdown server");
    // Close the socket server.
    drop(listener);
}
