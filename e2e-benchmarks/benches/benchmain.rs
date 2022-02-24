use criterion::criterion_main;

pub mod benchmarks;

criterion_main! {
    benchmarks::joinbench::joinbench,
}
