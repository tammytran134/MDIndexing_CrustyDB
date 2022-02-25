use escargot::CargoBuild;
use std::io::{Read, Result, Write};
use std::net::{Shutdown, TcpStream};
use std::process::Child;

use common::commands::{Commands, Response};

pub struct ServerWrapper {
    stream: TcpStream,
    child: Child,
}

impl ServerWrapper {
    fn setup_server() -> Result<Child> {
        let mut cargo_path = std::path::Path::new("../src/server/Cargo.toml");
        if !cargo_path.exists() {
            //Hack for debuging e2e tests
            cargo_path = std::path::Path::new("./src/server/Cargo.toml");
        }
        assert!(cargo_path.exists());
        CargoBuild::new()
            .bin("server")
            .current_release()
            .current_target()
            .manifest_path(cargo_path)
            .run()
            .unwrap()
            .command()
            // .stderr(Stdio::null())
            // .stdout(Stdio::null())
            .spawn()
    }

    fn try_connect() -> Result<TcpStream> {
        let bind_addr = "127.0.0.1:3333".to_string();
        let stream = TcpStream::connect(bind_addr)?;
        stream.set_nodelay(true).unwrap();
        Ok(stream)
    }

    pub fn new() -> std::result::Result<ServerWrapper, String> {
        // Configure log environment
        let child = ServerWrapper::setup_server().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        match ServerWrapper::try_connect() {
            Ok(stream) => Ok(ServerWrapper { stream, child }),
            _ => Err("Failed to connect to server".to_owned()),
        }
    }

    pub fn close_client(&mut self) {
        println!("Sending close...");
        //self.run_command_without_out("\\close");
        println!("Done...");
        self.stream
            .shutdown(Shutdown::Both)
            .expect("Shutdown occurred unsuccessfully");
        std::thread::sleep(std::time::Duration::from_millis(100));
        println!("About to kill client/server");
        self.child.kill().unwrap();
    }

    pub fn cleanup(&mut self) -> &mut Self {
        self.run_command(&Commands::Reset)
    }

    pub fn run_command_without_out(&mut self, command: &str) {
        // Send command
        self.stream
            .write_all(format!("{}\n", command).as_bytes())
            .expect("Failed to write");
    }

    pub fn run_command_with_out(&mut self, command: &Commands) -> Response {
        // Send command
        println!("-Running: {:?}", command);
        let bytes = serde_cbor::to_vec(command).unwrap();
        self.stream.write_all(&bytes).expect("Failed to write");

        // Read server response
        // the buffer must be large enough to hold all result bytes,
        // otherwise there will be a parsing error
        let mut data = [0; 1024*1024];
        let size = self.stream.read(&mut data[..]).unwrap();
  
        if size == 0 {
            Response::Err(String::from("Empty Response"))
        } else {
            serde_cbor::from_slice(&data[..size]).unwrap()
        }
    }

    pub fn run_command(&mut self, command: &Commands) -> &mut Self {
        println!("{:?}", self.run_command_with_out(command));
        self
    }
}
