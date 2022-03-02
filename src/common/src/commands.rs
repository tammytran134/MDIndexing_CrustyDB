/// Types of acceptable commands.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Commands {
    /// Create a database.
    Create(String),
    /// Connect to a database.
    Connect(String),
    /// Import a database.
    Import(String),
    /// Execute SQL statement
    ExecuteSQL(String),
    /// Register a query.
    RegisterQuery(String),
    /// Run a registered query upto a timestamp.
    RunQueryFull(String),
    /// Run a registered query for the diffs of a timestamp range.
    RunQueryPartial(String),
    /// Convert SQL to Json plan
    ConvertQuery(String),
    /// Show the tables of a database.
    ShowTables,
    /// Show the registered queries of a database.
    ShowQueries,
    /// List databases
    ShowDatabases,
    /// Resets the database.
    Reset,
    /// Shuts down the database
    Shutdown,
    /// Closes a connection
    CloseConnection,
    /// Go into quiet mode
    QuietMode,
    /// Generates CSV table
    Generate(String),
    /// Test
    Test,
}

/// Types of acceptable commands.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Msg(String),
    Err(String),
    QueryResult(crate::QueryResult),
    Shutdown,
    QuietOk,
    QuietErr,
}

/// Parses the command to determine which type of command it is.
///
/// We leave error handling to when we need to use the commands.
///
/// # Arguments
///
/// * `cmd` - Command to parse.
pub fn parse_command(mut cmd: String) -> Option<Commands> {
    if cmd.ends_with('\n') {
        cmd.pop();
        if cmd.ends_with('\r') {
            cmd.pop();
        }
    }

    //FIXME:ae maps commands to help/enum

    if !cmd.starts_with('\\') {
        return Some(Commands::ExecuteSQL(cmd));
    }

    if let Some(clean_cmd) = cmd.strip_prefix("\\r ") {
        // usage: \r <name>
        return Some(Commands::Create(clean_cmd.to_string()));
    } else if let Some(clean_cmd) = cmd.strip_prefix("\\c ") {
        // usage: \c <name>
        return Some(Commands::Connect(clean_cmd.to_string()));
    } else if let Some(clean_cmd) = cmd.strip_prefix("\\i ") {
        // usage: \i <path> <table_name>
        return Some(Commands::Import(clean_cmd.to_string()));
    } else if let Some(clean_cmd) = cmd.strip_prefix("\\register") {
        // usage: \register <query_json_path> <query_name>
        return Some(Commands::RegisterQuery(clean_cmd.to_string()));
    } else if let Some(clean_cmd) = cmd.strip_prefix("\\runFull") {
        // usage: \runFull <query_name> <cache> <timestamp>
        return Some(Commands::RunQueryFull(clean_cmd.to_string()));
    } else if let Some(clean_cmd) = cmd.strip_prefix("\\runPartial") {
        // usage: \runPartial <query_name> <cache> <start_timestamp> <end_timestamp>
        return Some(Commands::RunQueryPartial(clean_cmd.to_string()));
    } else if let Some(clean_cmd) = cmd.strip_prefix("\\convert") {
        // usage: \convert <query_json_path> | <sql>
        return Some(Commands::ConvertQuery(clean_cmd.to_string()));
    } else if cmd == "\\dt" {
        // usage: \dt
        return Some(Commands::ShowTables);
    } else if cmd == "\\dq" {
        // useage: \dq
        return Some(Commands::ShowQueries);
    } else if cmd == "\\l" {
        // usage: \l
        return Some(Commands::ShowDatabases);
    } else if cmd == "\\reset" {
        // usage: \reset
        return Some(Commands::Reset);
    } else if let Some(clean_cmd) = cmd.strip_prefix("\\generate") {
        // usage: \generate <csvname> <number of records>
        return Some(Commands::Generate(clean_cmd.trim().to_string()));
    } else if cmd == "\\t" {
        return Some(Commands::Test);
    } else if cmd == "\\shutdown" {
        return Some(Commands::Shutdown);
    } else if cmd == "\\quiet" {
        return Some(Commands::QuietMode);
    } else {
        info!("Invalid command received {}", cmd);
    }

    None
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create() {
        let create: String = String::from("\\r name");
        assert_eq!(
            Commands::Create("name".to_string()),
            parse_command(create).unwrap()
        );
    }

    #[test]
    fn test_connect() {
        let connect: String = String::from("\\c name");
        assert_eq!(
            Commands::Connect("name".to_string()),
            parse_command(connect).unwrap()
        );
    }

    #[test]
    fn test_import() {
        let import: String = String::from("\\i path name");
        assert_eq!(
            Commands::Import("path name".to_string()),
            parse_command(import).unwrap()
        );
    }

    #[test]
    fn test_reset() {
        let reset: String = String::from("\\reset\n");
        assert_eq!(Commands::Reset, parse_command(reset).unwrap());
    }

    #[test]
    fn test_show_tables() {
        let show_tables: String = String::from("\\dt\n");
        assert_eq!(Commands::ShowTables, parse_command(show_tables).unwrap());
    }
}
