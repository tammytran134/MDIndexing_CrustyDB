use crate::serverwrapper::ServerWrapper;
use common::commands::{parse_command, Commands, Response};
use common::prelude::*;
use rand::Rng;
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub struct Template {
    pub setup: Vec<Commands>,
    commands: Vec<Commands>,
    cleanup: Vec<Commands>,
    server: ServerWrapper,
}

impl Default for Template {
    fn default() -> Self {
        Self::new()
    }
}

impl Template {
    pub fn new() -> Template {
        let setup = vec![
            Commands::Create(String::from("db")),
            Commands::Connect(String::from("db")),
        ];

        Template {
            setup,
            commands: Vec::new(),
            cleanup: Vec::new(),
            server: ServerWrapper::new().unwrap(),
        }
    }

    pub fn string_to_setup(&mut self, sql: String) {
        let cmd = parse_command(sql).unwrap();
        self.setup.push(cmd);
    }

    pub fn show_configuration(&self) {
        println!("setup: {:?}", &self.setup);
        println!("commands: {:?}", &self.commands);
        println!("cleanup: {:?}", &self.cleanup);
    }

    pub fn generate_random_table(&mut self, name: &str, columns: i32, rows: i32) -> Vec<Tuple> {
        let mut rng = rand::thread_rng();

        let mut tuples: Vec<Tuple> = Vec::new();
        for _ in 0..rows {
            let mut fields: Vec<Field> = Vec::new();
            for _ in 0..columns {
                fields.push(Field::IntField(rng.gen_range(0, i32::MAX)));
            }
            tuples.push(Tuple::new(fields));
        }
        self.push_table(name, columns, &tuples);

        tuples
    }

    pub fn push_table(&mut self, name: &str, columns: i32, tuples: &[Tuple]) {
        let mut fs = "(".to_owned();
        for i in 0..columns {
            fs.push_str(&format!("f{} int,", i));
        }
        fs.push_str("primary key (");
        for i in 0..columns {
            fs.push_str(&format!("f{}", i));
        }
        fs.push_str("))");

        self.create_import_file(name.to_owned(), tuples);

        self.setup.push(Commands::ExecuteSQL(format!(
            "create table {} {}",
            name, fs
        )));
        self.setup
            .push(Commands::Import(format!("../{}.txt {}", name, name)));
    }

    fn create_import_file(&self, name: String, tuples: &[Tuple]) {
        let mut res = String::new();
        for tup in tuples.iter() {
            for field in tup.field_vals() {
                let val = match field {
                    Field::IntField(i) => i.to_string(),
                    Field::StringField(s) => s.to_string(),
                };
                res.push_str(&val);
                res.push(',');
            }
            res.push('\n');
        }

        let mut file = File::create(Path::new((name + ".txt").as_str())).unwrap();
        file.write_all(res.as_bytes()).unwrap();
    }

    pub fn add_command(&mut self, command: Commands) {
        self.commands.push(command);
    }

    pub fn run_setup(&mut self) {
        for command in self.setup.iter() {
            println!("Running command: {:?}", command);
            self.server.run_command(command);
        }
    }

    pub fn run_command_with_out(&mut self, command: &Commands) -> Response {
        self.server.run_command_with_out(command)
    }

    pub fn run_command(&mut self, command: &Commands) {
        self.server.run_command(command);
    }

    pub fn run_commands(&mut self) {
        //println!("# commands to run: {:?}", self.commands);
        for command in self.commands.iter() {
            println!("Running command: {:?}", command);
            self.server.run_command(command);
        }
    }

    pub fn run_cleanup(&mut self) {
        // println!("rust_cleanup...");
        for command in self.cleanup.iter() {
            println!("Running command: {:?}", command);
            self.server.run_command(command);
        }
        // send reset command
        self.server.cleanup();
        self.server.close_client();
        // println!("rust_cleanup...OK");
    }

    pub fn reset(&mut self) -> &mut Self {
        self.server.run_command(&Commands::Reset);
        self.run_setup();
        self
    }
}

impl Drop for Template {
    fn drop(&mut self) {
        self.run_cleanup();
    }
}
