#[macro_use]
extern crate log;
use env_logger::Env;
extern crate clap;
use clap::{App, Arg};

mod sqllogictest;
use std::path::Path;
use utilities::sqllogictest_utils::run_sqllogictests_in_file;

/// Entry point for e2e tests.
///
fn main() {
    // Configure log environment
    env_logger::from_env(Env::default().default_filter_or("warning")).init();

    const TEST_FILE_DIR: &str = "testdata";
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("test")
                .short("t")
                .long("test")
                .value_name("test")
                .help("Sets a testfile to use (from testdata)")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    if let Some(test_file_name) = matches.value_of("test") {
        let test_file_path = Path::new(TEST_FILE_DIR).join(test_file_name);
        let result = run_sqllogictests_in_file(&test_file_path);
        if let Err(e) = result {
            error!("Error running test : {:?}", e);
        } else {
            info!("Test {} ran", test_file_name);
        }
    }
}
