use common::commands::parse_command;
use common::commands::{Commands, Response};
use common::CrustyError;
//use common::Field;
use crate::template::Template;
use common::QueryResult;
use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(PartialEq, Debug)]
struct SQLLogicTest {
    command: Commands,
    expectation: SQLLogicTestResult,
}

#[derive(PartialEq, Debug)]
enum SQLLogicTestResult {
    Ok,
    Err,
    Query(QueryResult),
    ResultMatch(bool, PathBuf),
}

fn parse_sqllogictest(test_str: &str) -> Result<SQLLogicTest, CrustyError> {
    // change this unwrap to error
    let (expectation_str, rest) = test_str.split_once('\n').unwrap();

    let statement_ok = Regex::new("statement *ok").unwrap();
    let statement_err = Regex::new("statement *err").unwrap();
    let statement_omatch = Regex::new(r"omatch (.*\.csv)").unwrap();
    let statement_match = Regex::new(r"match (.*\.csv)").unwrap();
    // currently matches nothing, change this
    let query = Regex::new("$^").unwrap();

    let expectation = if statement_ok.is_match(expectation_str) {
        SQLLogicTestResult::Ok
    } else if statement_err.is_match(expectation_str) {
        SQLLogicTestResult::Err
    } else if statement_omatch.is_match(expectation_str) {
        //info!("omatch");
        let mut path: PathBuf = PathBuf::new();
        let caps = statement_omatch.captures(expectation_str).unwrap();
        path.push(caps.get(1).unwrap().as_str());
        SQLLogicTestResult::ResultMatch(true, path)
    } else if statement_match.is_match(expectation_str) {
        //info!("match");
        let mut path: PathBuf = PathBuf::new();
        let caps = statement_match.captures(expectation_str).unwrap();
        path.push(caps.get(1).unwrap().as_str());
        SQLLogicTestResult::ResultMatch(false, path)
    } else if query.is_match(expectation_str) {
        // update this to contain the seen output
        SQLLogicTestResult::Query(QueryResult::new(""))
    } else {
        return Err(CrustyError::CrustyError(format!(
            "first line of test pair {} does not match any expectation types",
            expectation_str
        )));
    };

    let mut tokens = rest.split("----");
    let possible_command_str = tokens.next();
    if possible_command_str.is_none() {
        return Err(CrustyError::CrustyError(String::from(
            "Expectation must be followed by a command to run",
        )));
    }

    let command = match parse_command(possible_command_str.unwrap().to_string()) {
        Some(command) => command,
        None => return Err(CrustyError::CrustyError(String::from("Invalid command"))),
    };
    /*
    let possible_query_result_str = tokens.next();
    let output = if let SQLLogicTestResult::Query(_) = expectation {
        match possible_query_result_str {
            Some(query_result_str) => Some(QueryResult::new(query_result_str)),
            None => {
                return Err(CrustyError::CrustyError(String::from(
                    "Tests with expected type query must have an output",
                )))
            }
        }
    } else {
        match possible_query_result_str {
            Some(_) => {
                return Err(CrustyError::CrustyError(String::from(
                    "Tests with expected ok or err cannot have output",
                )))
            }
            None => None,
        }
    };
    */

    Ok(SQLLogicTest {
        command,
        expectation,
    })
}

fn parse_sqllogictests(tests_str: &str) -> Result<Vec<SQLLogicTest>, CrustyError> {
    let lines: Vec<&str> = tests_str
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    let split_indices = get_split_indices(&lines);

    let mut res = Vec::new();
    for (start, finish) in split_indices.iter().zip(split_indices.iter().skip(1)) {
        let len = *finish - *start;
        let joined = lines
            .iter()
            .clone()
            .skip(*start)
            .take(len)
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join("\n");
        res.push(parse_sqllogictest(&joined)?);
    }
    Ok(res)
}

fn get_split_indices(lines: &[&str]) -> Vec<usize> {
    let mut res = Vec::new();

    let statement = Regex::new(r"statement \w+").unwrap();
    let matched = Regex::new(r"\w?match").unwrap();
    // currently matches nothing, change this
    let query = Regex::new("$^").unwrap();

    for (i, line) in lines.iter().enumerate() {
        if statement.is_match(line) || matched.is_match(line) || query.is_match(line) {
            res.push(i);
        }
    }
    res.push(lines.len());
    res
}

// convert commands give a <json name>  | <sql>
// this looks for commands with | in it to remove the json file
fn get_created_file(test: &SQLLogicTest, result: &SQLLogicTestResult) -> Option<String> {
    if let (Commands::ConvertQuery(args), SQLLogicTestResult::Ok) = (&test.command, &result) {
        let mut tokens = args.split('|');
        tokens.next().map(|s| s.trim().to_string())
    } else {
        None
    }
}

fn compare_csvs(
    header_present: bool,
    query_res_csv: &str,
    given_csv: String,
) -> Result<(), CrustyError> {
    let skip_count = match header_present {
        true => 1,
        false => 0,
    };
    let mut query_lines: Vec<&str> = query_res_csv.split('\n').skip(skip_count).collect();
    let given_lines = given_csv.split('\n');
    let mut count_found = 0;
    for given in given_lines {
        let idx = query_lines.iter().position(|x| *x == given);
        match idx {
            Some(pos) => {
                // found an instance of this string, remove
                query_lines.remove(pos);
                count_found += 1;
            }
            None => {
                return Err(CrustyError::CrustyError(format!(
                    "Did not find query result: {} in unordered check of {:?}. Successfully found {} so far (stopping).",
                    given, query_lines, count_found
                )));
            }
        }
    }
    if !query_lines.is_empty() {
        return Err(CrustyError::CrustyError(format!(
            "Found all given expected records in unordered check. These records were in the query but not in the expected given results\n{:?}\nSuccesfully found {} so far (stopping).",
            query_lines, count_found
        )));
    }
    Ok(())
}

fn run_sqllogictests(tests: &[SQLLogicTest]) -> Result<(), CrustyError> {
    let mut template = Template::new();
    template.run_setup();

    let mut created_files = Vec::new();

    for test in tests {
        let output = template.run_command_with_out(&test.command);
        let result = response_to_integration_test_result(&output);
        if let Some(created_file) = get_created_file(test, &result) {
            created_files.push(created_file);
        }

        if let SQLLogicTestResult::ResultMatch(ordered, path) = &test.expectation {
            if let Response::QueryResult(qr) = &output {
                debug!(" Need to compare {:?} against {:?}", path, qr.result);
                let contents = fs::read_to_string(path)?;
                if *ordered {
                    if contents != qr.result {
                        return Err(CrustyError::CrustyError(format!(
                            "Looking at ordered comparison test file gave:\n {}\n got:\n{:?}",
                            contents, qr.result,
                        )));
                    }
                } else {
                    compare_csvs(false, &qr.result, contents)?;
                }
            }
        } else if result != test.expectation {
            let err = format!("Failure: test {:?} got {:?}", test, output,);
            error!("{}", err);
            // deleting physical plan files created while running this test
            for created_file in created_files {
                fs::remove_file(created_file).unwrap();
            }
            return Err(CrustyError::CrustyError(err));
        }
    }

    // deleting physical plan files created while running this test
    for created_file in created_files {
        fs::remove_file(created_file).unwrap();
    }

    Ok(())
}

fn response_to_integration_test_result(response: &Response) -> SQLLogicTestResult {
    match response {
        Response::Err(_) | Response::QuietErr => SQLLogicTestResult::Err,
        _ => SQLLogicTestResult::Ok,
    }
}

pub fn run_sqllogictests_in_file(path: &Path) -> Result<(), CrustyError> {
    let tests_str = &fs::read_to_string(path)?;
    let tests = parse_sqllogictests(tests_str)?;
    info!("Tests to run {:?}", tests);
    run_sqllogictests(&tests)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_parse_sqllogictest() {
        // tests from cockroach
        let test_str1 = "statement ok\nCREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)";
        let expected1 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Commands::ExecuteSQL(String::from(
                "CREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)",
            )),
        };
        assert_eq!(parse_sqllogictest(test_str1).unwrap(), expected1);

        let test_str2 = "statement err\nCREATE TABLE kv (k INT, v INT, w INT, s STRING)";
        let expected2 = SQLLogicTest {
            expectation: SQLLogicTestResult::Err,
            command: Commands::ExecuteSQL(String::from(
                "CREATE TABLE kv (k INT, v INT, w INT, s STRING)",
            )),
        };
        assert_eq!(parse_sqllogictest(test_str2).unwrap(), expected2);
        /*
        let test_str3 = "query\nSELECT min(1), max(1), count(1) FROM kv----NULL NULL 0";
        let expected3 = SQLLogicTest {
            expectation: SQLLogicTestResult::Query(QueryResult::new("")),
            command: Commands::ExecuteSQL(String::from("SELECT min(1), max(1), count(1) FROM kv")),
        };
        assert_eq!(parse_test(test_str3).unwrap(), expected3);
        */

        // tests using commands
        let test_str4 = "statement ok\n\\dq";
        let expected4 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Commands::ShowQueries,
        };
        assert_eq!(parse_sqllogictest(test_str4).unwrap(), expected4);

        let test_str5 = "statement ok\n\\convert data.csv | select * from test";
        let expected5 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Commands::ConvertQuery(String::from(" data.csv | select * from test")),
        };
        assert_eq!(parse_sqllogictest(test_str5).unwrap(), expected5);

        // tests that should fail
        let test_str6 = "statement ok\n\\run";
        assert!(parse_sqllogictest(test_str6).is_err());

        /*
        let test_str7 = "query\nselect * from test";
        assert!(parse_test(test_str7).is_err());


        let test_str8 = "statement ok\n\\dt----NULL NULL 0";
        assert!(parse_test(test_str8).is_err());
        */
    }

    #[test]
    fn test_parse_filter() {
        let contents = r#"statement ok
        create table test (a int primary key, b int)
        
        statement ok
        \i csv/data.csv test
        
        match csv/filter1.csv
        select * from test where test.a = 1
        
        statement ok
        \reset
        "#;
        let parsed = parse_sqllogictests(contents);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_omatch_sqllogic() {
        let test_str = "statement ok\nCREATE TABLE test (a int primary key, b int)\n\nstatement ok\n\\i csv/data.csv test\n\nomatch csv/data.csv\nselect * from test";
        let expected = vec![
            SQLLogicTest {
                expectation: SQLLogicTestResult::Ok,
                command: Commands::ExecuteSQL(String::from(
                    "CREATE TABLE test (a int primary key, b int)",
                )),
            },
            SQLLogicTest {
                expectation: SQLLogicTestResult::Ok,
                command: Commands::Import(String::from("csv/data.csv test")),
            },
            SQLLogicTest {
                expectation: SQLLogicTestResult::ResultMatch(true, PathBuf::from("csv/data.csv")),
                command: Commands::ExecuteSQL(String::from("select * from test")),
            },
        ];
        assert_eq!(parse_sqllogictests(test_str).unwrap(), expected)
    }

    #[test]
    fn test_parse_sqllogictests() {
        let tests_str = "statement err\nCREATE TABLE kv (k INT, v INT, w INT, s STRING)\nstatement ok\nCREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)\n# Aggregate functions return NULL if there are no rows.\n";
        let expected = vec![
            SQLLogicTest {
                expectation: SQLLogicTestResult::Err,
                command: Commands::ExecuteSQL(String::from(
                    "CREATE TABLE kv (k INT, v INT, w INT, s STRING)",
                )),
            },
            SQLLogicTest {
                expectation: SQLLogicTestResult::Ok,
                command: Commands::ExecuteSQL(String::from(
                    "CREATE TABLE kv (k INT PRIMARY KEY, v INT, w INT, s STRING)",
                )),
            },
        ];
        assert_eq!(parse_sqllogictests(tests_str).unwrap(), expected)
    }
    /*
    #[test]
    fn test_run_tests() {
        let test1 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Commands::ExecuteSQL(String::from("CREATE TABLE test (a INT, b INT)")),
        };
        let test2 = SQLLogicTest {
            expectation: SQLLogicTestResult::Err,
            command: Commands::ExecuteSQL(String::from("CREATE TABLE test (a INT, b INT)")),
        };
        let test3 = SQLLogicTest {
            expectation: SQLLogicTestResult::Ok,
            command: Commands::ExecuteSQL(String::from(
                "CREATE TABLE test (a INT primary key, b INT)",
            )),
        };
        let test4 = SQLLogicTest {
            expectation: SQLLogicTestResult::Err,
            command: Commands::ExecuteSQL(String::from(
                "CREATE TABLE test (a INT primary key, b INT)",
            )),
        };

        run_tests(vec![test2.clone(), test3.clone(), test4.clone()]).unwrap();
        assert!(run_tests(vec![test1.clone()]).is_err());
        assert!(run_tests(vec![test3.clone(), test3.clone()]).is_err());
        assert!(run_tests(vec![test4.clone()]).is_err());
    }
    */
}
