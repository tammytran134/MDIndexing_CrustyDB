/// Types of acceptable commands.
#[derive(Debug, PartialEq)]
pub enum Commands {
    /// Create a table.
    Create(String),
    /// Connect to a database.
    Connect(String),
    /// Import a database.
    Import(String),
    /// Show the tables of a database.
    ShowTables,
    /// List databases
    ShowDatabases,
    /// Resets the database.
    Reset,
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

    if cmd.starts_with("\\r ") {
        // usage: \r <name>
        return Some(Commands::Create(cmd[3..].to_string()));
    } else if cmd.starts_with("\\c ") {
        // usage: \c <name>
        return Some(Commands::Connect(cmd[3..].to_string()));
    } else if cmd.starts_with("\\i ") {
        // usage: \i <path> <table_name>
        return Some(Commands::Import(cmd[3..].to_string()));
    } else if cmd == "\\d" {
        // usage: \d
        //return Some(Commands::Reset);
        return None;
    } else if cmd == "\\dt" {
        // usage: \dt
        return Some(Commands::ShowTables);
    } else if cmd == "\\l" {
        // usage: \l
        return Some(Commands::ShowDatabases);
    } else if cmd == "\\reset" {
        // usage: \l
        return Some(Commands::Reset);
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
        let reset: String = String::from("\\d\n");
        assert_eq!(Commands::Reset, parse_command(reset).unwrap());
    }

    #[test]
    fn test_show_tables() {
        let show_tables: String = String::from("\\dt\n");
        assert_eq!(Commands::ShowTables, parse_command(show_tables).unwrap());
    }
}
