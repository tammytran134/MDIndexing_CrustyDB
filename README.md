# CrustyDB

CrustyDB is an academic Rust-based relational database management system built
by ChiData at The University of Chicago. You should read this document fully and look at the CrustyDB.pdf.


## Usage

Make sure you have Rust > 1.57.0. Updating the rust toolkit is pretty easy, just do:

```bash
$ rustup update
```

You can then check the version by doing:

```bash
$ rustc --version
```

### CSIL / vdesk etc.
By default the cargo/rust version on cs.linux or vdesk is out of date. However, techstaff has set up a way
for you to use 1.49. You must run the following command on login.

```
$ module load rust
```

You can verify this worked with
```
$ rustc --version
rustc 1.58.0 (02072b482 2022-01-11)
$ cargo --version
cargo 1.58.0 (7f08ace4f 2021-11-24)
```

## Building project
To build the entire CrustyDB source code, you would run `cargo build`

CrustyDB is set up as a workspace and various modules/components of the database are broken into separate packages/crates. To build a specific crate (for example common), you would use the following command `cargo build -p common`. Note if a package/crate depends on another crate (e.g. heapstore depends on common and txn_manager) those crates will be built as part of the process.

These crates are:
- `cli-crusty` : a command line interface client binary application that can connect and issue commands/queries to a running CrustyDB server.
- `common` : shared data structures or logical components needed by everything in CrustyDB. this includes things like tables, errors, logical query plans, ids, some test utilities, etc. `use use common::prelude::*;` will include many frequently used structs/types/enums in your rust module/file.
- `heapstore` : a storage manager for storing data in pages and in heap files. milestone `pg` and `hs` is exclusively in this crate. this will be a replacement for the memstore in server.
- `memstore` : a poorly written storage manager that keeps everything in memory. it will persist data to files using serde on shutdown, and use these files to recreate the database state at shutdown
- `optimizer` : a crate for generating the query execution plan and for query optimization
- `queryexe` : responsible for executing queries. this contains the operator implementations as well as the execution code.
- `server` : the binary crate for running a CrustyDB server. this will glue all modules (outside a client) together.
- `txn_manager` : a near empty crate for an optional milestone to implement transactions. the use a `transaction` is embedded in many other crates, but can be safely ignored for the given milestones. There is also the use of a logical timestamp throughout many components. You can safely ignore this.
- `utilities` : utilities for performance benchmarks that will be used by an optional milestone

There are two other projects outside of crustydb workspace that we will use later `e2e-benchmarks` and `e2e-tests`. These are used for end-to-end testing (eg sending SQL to the server and getting a response).

## Things to ignore

We are planning on using this database system for other classes (including the new advanced DB systems course at UChicago) along with using the framework for research (reach out if you are interested). Therefore, you may come across some variables, types, or functions that you do not need. For 23500 you can safely ignore Transactions/TransactionIds/tids, logical time stamps, registered queries (e.g. views), the `segment_id` in `ValueId`, and deltas/versions. If you want the rust compiler to stop complaining about an unused variable, change the name so it begins with an underscore (e.g. `tid` -> `_tid`), this tells the compiler that you know the variable is unused. 

## Tests

Most crates have tests that can be run using cargo `cargo test`. Like building you can run tests for a single crate `cargo test -p common`. Note that tests will build/compile code in the tests modules, so you may encounter build errors here that do not show up in a regular build.


### Running an ignored test
Some longer tests are set to be ignored by default. To run them: `cargo test -- --ignored`

## Logging

CrustyDB uses the [env_logger](https://docs.rs/env_logger/0.8.2/env_logger/) crate for logging.  You should use logging instead of print! for capturing 
messages/debugging.

Per the docs on the log crate:
```
The basic use of the log crate is through the five logging macros: error!, warn!, info!, debug! and trace! 
where error! represents the highest-priority log messages and trace! the lowest. 
The log messages are filtered by configuring the log level to exclude messages with a lower priority. 
Each of these macros accept format strings similarly to println!.
```

The logging level is set by an environmental variable, `RUST_LOG`. The easiest way to set the level is when running a cargo command you set the logging level in the same command. EG : `RUST_LOG=debug cargo run --bin server`. However, when running unit tests the logging/output is suppressed and the logger is not initialized. So if you want to use logging for a test you must:
- Make sure the test in question calls `init()` which is defined in `common::testutils` that initializes the logger. It can safely be called multiple times.
- Tell cargo to not capture the output. For example, setting the level to DEBUG: `RUST_LOG=debug cargo test -- --nocapture [opt_test_name]`  **note the -- before --nocapture**

Examples:
```
RUST_LOG=debug cargo run --bin server
RUST_LOG=debug cargo test
RUST_LOG=debug cargo test -- --nocapture [test_name]
```

In addition, the log level can also be controlled programmatically. The log
level is set in the first line of the main() function in the server crate. By
default, this is set to DEBUG. Feel free to change this as you see fit.

We have provided two shell scripts to run the server with info and debug (info-server.sh and debug-server.sh).

### Connecting to a Database

This is the basic process for starting a database and connecting to it via the CLI client.

1. Start a server thread

    ```
    $ cargo run --bin server
    ```

2. Start a client

    ```
    $ cargo run --bin cli-crusty
    ```

For convenience we have provided some shell scripts to run the server and client. The server has a debug and info mode for the logger.

### Client Commands

CrustyDB emulates psql commands.

Command | Functionality
---------|--------------
`\r [DATABABSE]` | cReates a new database, DATABASE
`\c [DATABASE]` | Connects to DATABASE
`\i [PATH] [TABLE_NAME]` | Imports a csv file at PATH and saves it to TABLE_NAME in 
whatever database the client is currently connected to.
`\l` | List the name of all databases present on the server.
`\dt` | List the name of all tables present on the current database.
`\generate [CSV_NAME] [NUMBER_OF_RECORDS]` | Generate a test CSV for a sample schema.
`\reset` | Calls the reset command. This should delete all data and state for all databases on the server
`\close` | Closes the current client, but leaves the database server running
`\shutdown` |  Shuts down the database server cleanly (allows the DB to gracefully exit)

There are other commands you can ignore for this class (register, runFull, runPartial, convert).

The client also handles basic SQL queries.

## End to End Example

After compiling the database, start a server and a client instance.

To start the crustydb server:

```
$ cargo run --bin server
```

and to start the client:

```
$ cargo run --bin cli-crusty
```

Now, from the client, you can interact with the server. Create a database named
'testdb':

```
[crustydb]>> \r testdb 
```

Then, connect to the newly created database:

```
[crustydb]>> \c testdb
```

At this point, you can create a table 'test' in the 'testdb' database you are
connected to by writing the appropriate SQL command. Let's create a table with 2
Integer columns, which we are going to name 'a' and 'b'.

```
[crustydb]>> CREATE TABLE test (a INT, b INT, primary key (a));
```

At this point the table exists in the database, but it does not contain any data. We include a CSV file in the repository (named 'data.csv') with some sample data you can import into the newly created table. You can do that by doing:

```
[crustydb]>> \i <PATH>/data.csv test
```

Note that you need to replace PATH with the path to the repository where the
data.csv file lives.

After importing the data, you can run basic SQL queries on the table. For
example:

```
[crustydb]>> SELECT a FROM test;
```

or:

```
[crustydb]>> SELECT sum(a), sum(b) FROM test;
```

As you follow through this end to end example, we encourage you to take a look
at the log messages emitted by the server. You can search for those log messages
in the code: that is a great way of understanding the lifecycle of query
execution in crustydb.

### Client Scripts 

The client has an option of running a series of commands/queries from a text file. 
Each command or query must be separated by a ; (even commands that would not give 
a ; after when using the cli tool). To use the script pass `-- -s [script file]` 

We have included a sample script that you would invoke the following way:
```
cargo run -p cli-crusty -- -s test-client-script
```

## (Reminder from Primer) Debugging Rust Programs

Debugging is a crucial skill you should learn (if you don't know yet) in order
to become a more effective software developer. If you write software, your
software will contain bugs. Debugging is the process of finding those bugs,
which is necessary if you want to fix them.

### Debuggers

There are tools to help you debug software called debuggers. You may
have already heard about these. For example, in the C, C++ world, gdb and lldb
are two popular debuggers. gdb is used to debug programs that have been compiled
with gcc, while lldb is used to debug programs compiled with the LLVM toolkit.
What this means in practice, in 2020, is that if you work on a Linux platform,
you'll likely be using gdb. If you work on a Mac OS platform, you'll likely be
using lldb. If you work on a windows platform, then you may be using either one,
depending on your configuration.

### Debuggers in the IDE

A popular way of writing software is via IDEs (which we recommend you use to
develop crustyDB). IDEs for most languages come with a debugger preconfigured.
The situation for Rust is a little different. Only relatively recently IDE
developers have started incorporating debuggers for Rust, and the support is
still sparse. If you use Visual Studio Code (a lightweight and open source IDE),
you will be able to use a Rust debugger (based on gdb or lldb depending on the
underlying platform). You can easily find instructions online on how to set this
up.

Most other free IDEs do not have good support for the Rust debugger yet.

#### CLion

JetBrain's [CLion](https://www.jetbrains.com/clion/) IDE looks to have a solid Rust debugger with the Rust extension.
However CLion is not free, but it does offer academic licenses. [Apply here](https://www.jetbrains.com/community/education/#students)
if you want to access the tool (some restrictions on what you can use the tool for).
[Here are instructions on set up and using](https://blog.jetbrains.com/clion/2019/10/debugging-rust-code-in-clion/)
which worked for me out of the box on Ubuntu (with installing the Rust plugin).
One of our TAs uses CLion to debug Rust on OSX. The link also contains instructions for 
debugging on Windows, but it has not been tested by us.

#### VSCode

We have had some mixed success with using VSCode for debugging Rust
(although it is a great Rust IDE with the right extensions).  Using the
extensions Rust and CodeLLDB on Ubuntu has gotten debugging working on a set up.
We included the launch.json for running tests in a package.

### Alternative ways of debugging programs

You are already familiar with printing the values of variables in your programs
in order to understand program behavior and detect problems, i.e., in order to
debug your programs. Rust has its own println!() macro (and Crusty uses a
logging library). Rust also has a dbg!()
macro in its standard library, which will simply format the argument so its
printable along with the line where it's found.  A real debugger will give you
much more information, presented better, and in context, so it's a much more
powerful way of debugging programs, and the recommended way. However, in some
instances, the macros above may come in handy, especially as Rust's debuggers
support matures.
