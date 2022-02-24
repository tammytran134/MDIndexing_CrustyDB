use crate::benchmarks::benchtemplate::BenchTemplate;
use common::commands::Commands;
use criterion::{criterion_group, Criterion};
use utilities::template::Template;

pub fn bench_filter_one_column_small(c: &mut Criterion) {
    println!("**filter small **");

    let mut bt = Template::new();
    //TODO
    bt.generate_random_table("a", 1, 100);
    bt.add_command(Commands::ExecuteSQL(String::from(
        "select * from a where a.f0 > 100000",
    )));
    bt.bench_server(c, "filter_one_column_small");
}

fn bench_filter_one_column_huge(c: &mut Criterion) {
    let mut bt = Template::new();
    bt.generate_random_table("a", 1, 10000);
    bt.add_command(Commands::ExecuteSQL(String::from(
        "select * from a where a.f0 < 100000",
    )));
    bt.bench_server(c, "filter_one_column_huge");
}

criterion_group! {
    name = filterbench;
    config = Criterion::default().sample_size(10);
    targets = bench_filter_one_column_small,
    bench_filter_one_column_huge,
}
