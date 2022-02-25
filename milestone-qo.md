# Query Operators Milestone (qo)

CrustyDB runs sequential scans out of the box. In this milestone, you will
implement the *aggregate*, *groupby*, and *join* operators, so you can execute more
complex queries.  As in the first milestone, we provide a suite of tests that
your implementation must pass. 

This milestone is less guided than the first. However, we still want to explain
a bit about the process of implementing these operators. The points below are
geared towards that goal.

## Query operators' logic

The very first thing that you need to have clear is how the join,
aggregate, and groupby operators work. This is independent of CrustyDB. You
should think hard what each of these operators needs to do with the data that is
fed to them for them to produce the correct output. It probably helps writing
some pseudocode on paper, sketching a quick implementation in your favorite
scripting language, or just having a clear idea of the algorithm behind each of
these operators. In general, conceptually, each logical operator is simple. In
practice, implementations can get arbitrarily complicated, depending on how much
you care about optimizing for performance, for example. In this milestone, we
are not measuring performance, just correctness.

## Execution Engine

In RDBMSs, including CrustyDB, operators do not run in a vacuum. Instead, as
we've discussed in class, they become the nodes of a query plan that is executed
by an execution engine. There are many kinds of execution engines and many ways
of implementing query plans. CrustyDB's execution engine implements a
Volcano-style interface with *open*, *next*, etc. This means that your
implementation of *aggregate*, *groupby*, and *join* will need to implement this
inteface (we have a Trait, OpIterator, in the Rust-based CrustyDB implementation) 
as well so it can be integrated within CrustyDB's execution engine.

Hint. Take a look at how SQL queries get parsed and translated into logical
query plans (queryexe/query/translate_and_validate.rs). Then, take a look at how
these plans are executed by studying queryexe/query/executor.rs.

## OpIterator Trait

We use Rust's Trait system to represent the Volcano-style operator interfaces.
You can find the definition in the OpIterator Trait
(queryexe/opiterator/mod.rs),
which every operator in CrustyDB implements. Furthermore, you should take a look
at the operators we have implemented for you to understand how this interface is
used in practice.

If you have set up a debugger, this is a great time to put it to use: it'll help
you trace what happens during query execution (debuggers are not only useful to
find bugs).

After you have understood the lifecycle of query execution, and once you have a
clear idea of what the *aggregate*, *groupby*, and *join* operators must do,
then it is time to implement them!

## Guide to Implementing Aggregate and Join

Unlike a sequential scan, aggregate and join operators are *stateful* and
*blocking*. They are stateful because their output depends on their input and on
some *state* that the operator must manage. They are blocking because they
cannot produce output until they have seen the entire input data. If
these two concepts seem difficult, then I encourage you to write in pseudocode
the aggregate and join operators before jumping into the real implementation.
These two ideas should be very clear in your mind!

## End-To-End (e2e) Testing
We have given a new directory/project for automating e2e testing of queries.
This project allows for a test file to be specified that can list a series of 
commands and queries, with expected results. More details are coming soon on these 
tests.  You can run all tests by the following command

```
cd e2e-tests
sh runTests.sh
```

This will execute every test in e2e-tests/testdata.
Note the test `run_sql_logic_tests` attempts to run all of the tests in the testdata directory. 
The test `run_sql_join` is redundant, and runs a single test (which is covered by the full test suite).
We include this so you can see how to run one test if needed for debugging. 
You should also note that these tests work by running a client and server. If something goes wrong and 
your server does not properly close/clean-up/reset you may leave a server process hung (running in
in the background.) When you try to run a test again you will see the port is blocked. You will need to identify
the process and kill it (it will likely be called server).  You might need to search for how to do this 
if you haven't. For example, this SO article shows how to do this based on a port for Mac: 
https://stackoverflow.com/questions/3855127/find-and-kill-process-locking-port-3000-on-mac .

## Scoring and Requirements

70% of your score on this milestone is based on correctness that is demonstrated
by passing all of the provided unit and integration tests in the queryexe crate.
This means when running `cargo test -p queryexe` all tests pass. 
10% of your score is based on whether we can run queries that include
aggregates, groupby, and joins end to end (so you should make sure this is
possible and you may want to write additional tests to harden your
implementation). 10% is based on code quality. 10% is based on your write 
up (my-op.txt). The write up should contain:

 -  A brief description of your solution. In particular, include what design
decisions you made and explain why. This is only needed for those parts of your
solutions that had some significant design work (e.g. how you implemented and handled
state in the aggregate and join operators). 

- How long you roughly spent on the milestone, and what would have
liked/disliked on the milestone.

- If you know some part of the milestone is incomplete, write up what parts are
not working, how close you think you are, and what part(s) you got stuck on.

## Additional Tips
If you run `cargo test -p queryexe` before implementing anything, you will see 33 passed tests. This is because several operators have been implemented. Your goal is to implement aggregate and join to pass the rest of the test cases without breaking other passed tests. If you want to run test cases under a particular operator - use `cargo test -p queryexe opiterator::<op to test>` , e.g. `cargo test -p queryexe opiterator::join`
