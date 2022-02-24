use crate::benchmarks::benchtemplate::BenchTemplate;
use common::commands::Commands;
use criterion::{criterion_group, Criterion};
use utilities::template::Template;

const BASE_PATH: &str = "../test_data/";

fn setup_tables(bt: &mut Template, left_table: &str, right_table: &str) {
    bt.string_to_setup("CREATE TABLE testA (a INT, b INT, primary key (a));".to_string());
    bt.string_to_setup(format!("\\i {}{}.csv testA", BASE_PATH, left_table));

    bt.string_to_setup("CREATE TABLE testB (a INT, b INT, primary key (a));".to_string());
    bt.string_to_setup(format!("\\i {}{}.csv testB", BASE_PATH, right_table));
}

fn bench_join_tiny(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_tables(&mut bt, "tiny_data", "tiny_data");
    bt.add_command(Commands::ExecuteSQL(String::from(
        "select * from testA join testB on testA.a = testB.a",
    )));
    bt.show_configuration();
    bt.bench_server(c, "join_tiny");
}

fn bench_join_small(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_tables(&mut bt, "small_data", "small_data");
    bt.add_command(Commands::ExecuteSQL(String::from(
        "select * from testA join testB on testA.a = testB.a",
    )));
    bt.show_configuration();
    bt.bench_server(c, "join_small");
}

fn bench_join_right(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_tables(&mut bt, "right_data", "left_data");
    bt.add_command(Commands::ExecuteSQL(String::from(
        "select * from testA join testB on testA.a = testB.a",
    )));
    bt.show_configuration();
    bt.bench_server(c, "join_right");
}

fn bench_join_left(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_tables(&mut bt, "right_data", "left_data");
    bt.add_command(Commands::ExecuteSQL(String::from(
        "select * from testB join testA on testB.a = testA.a",
    )));
    bt.show_configuration();
    bt.bench_server(c, "join_left");
}

fn bench_join_large(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_tables(&mut bt, "large_data", "large_data");
    bt.add_command(Commands::ExecuteSQL(String::from(
        "select * from testA join testB on testB.a = testA.a",
    )));
    bt.show_configuration();
    bt.bench_server(c, "join_large");
}

criterion_group! {
    name = joinbench;
    config = Criterion::default().sample_size(10);
    targets =
    bench_join_tiny,
    bench_join_small,
    bench_join_right,
    bench_join_left,
    bench_join_large,
}
