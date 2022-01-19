extern crate clap;
extern crate rustyline;
use clap::{App, Arg};
use env_logger::Env;
use log::{debug, error, info};
use serde::Deserialize;

use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};

use common::commands;
use common::commands::{Commands, Response};

#[derive(Deserialize, Debug)]
struct ClientConfig {
    host: String,
    port: String,
}

fn process_input(stream: &mut TcpStream, request: Commands) -> bool {
    let bytes = serde_cbor::to_vec(&request).unwrap();

    if let Err(x) = stream.write_all(&bytes) {
        error!("Error sending data {:?}", x);
        return false;
    }

    let mut data = [0; 1024];
    match stream.read(&mut data) {
        Ok(size) => {
            //TODO: Remove echo
            if size == 0 {
                info!("Received empty response. Check server logs");
                true
            } else {
                let response: Response = serde_cbor::from_slice(&data[0..size]).unwrap();
                debug!("Message received [{:?}]", response);
                match response {
                    Response::Shutdown => {
                        info!("Received Quit Command");
                        false
                    }
                    Response::Ok => {
                        info!("Received OK");
                        true
                    }
                    Response::Msg(msg) => {
                        info!("Received: {}", msg);
                        true
                    }
                    Response::Err(msg) => {
                        error!("Error: {}", msg);
                        true
                    }
                    Response::QueryResult(res) => {
                        info!("Received: {:?}", res);
                        true
                    }
                    Response::QuietOk => {
                        debug!("Received quiet OK");
                        true
                    }
                    Response::QuietErr => {
                        debug!("Received quiet Err");
                        true
                    }
                }
            }
        }
        Err(x) => {
            error!("Error received {:?}", x);
            true
        }
    }
}

#[allow(unused_must_use)]
fn process_cli_input(stream: &mut TcpStream) {
    let mut rl = Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        info!("No previous history.");
    }
    let prompt: &str = "[crustydb]>>";
    let mut cont = true;
    while cont {
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                if line.as_str() == "" {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                match commands::parse_command(line) {
                    Some(request) => {
                        debug!("Request to send {:?}", request);
                        cont = process_input(stream, request);
                    }
                    None => {
                        info!("Invalid request");
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                info!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                info!("CTRL-D");
                break;
            }
            Err(err) => {
                error!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt").unwrap();

    //TODO: error handle on shutdown.
    stream.shutdown(Shutdown::Both);
}

#[allow(unused_must_use)]
fn process_script_input(stream: &mut TcpStream, script: String) {
    let lines = script.split(';');
    for line in lines {
        let command = line.trim();
        if command.is_empty() {
            continue;
        }
        let clean_command = &command.replace("\n", " ");
        info!("Script clean command: {}", clean_command);
        match commands::parse_command(clean_command.to_string()) {
            Some(request) => {
                debug!("Request to send {:?}", request);
                if !process_input(stream, request) {
                    panic!("Bad Script");
                }
            }
            None => {
                info!("Invalid request");
            }
        }
    }

    //TODO: error handle on shutdown.
    stream.shutdown(Shutdown::Both);
}

fn main() {
    // Configure log environment
    env_logger::from_env(Env::default().default_filter_or("info")).init();

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
                .default_value("0.0.0.0")
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
            Arg::with_name("script")
                .short("s")
                .long("script")
                .value_name("CRUSTY_SCRIPT")
                .help("Takes in a semicolon delimited file of crusty commands and SQL queries.")
                .takes_value(true)
                .required(false),
        )
        .get_matches();

    let config = if let Some(c) = matches.value_of("config") {
        let config_path = c;
        let contents = fs::read_to_string(config_path).unwrap();
        serde_json::from_str(&contents).unwrap()
    } else {
        let host = matches.value_of("host").unwrap();
        let port = matches.value_of("port").unwrap();
        ClientConfig {
            host: host.to_string(),
            port: port.to_string(),
        }
    };

    info!("Starting client with config: {:?}", config);

    let script: String = if let Some(s) = matches.value_of("script") {
        let script_path = s;
        fs::read_to_string(script_path).expect("Cannot find or read input script file")
    } else {
        String::new()
    };

    let mut bind_addr = config.host.clone();
    bind_addr.push(':');
    bind_addr.push_str(&config.port);

    match TcpStream::connect(bind_addr) {
        Ok(mut stream) => {
            if script.is_empty() {
                process_cli_input(&mut stream);
            } else {
                process_script_input(&mut stream, script);
            }
        }
        Err(e) => {
            error!("Failed to connect: {}", e);
        }
    }
    info!("Terminated.");
}
