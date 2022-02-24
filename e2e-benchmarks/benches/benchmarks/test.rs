use criterion::{black_box, Criterion};

pub fn count_benchmark(c: &mut Criterion) {
    let counter = 10000;
    println!("**count **");

    c.bench_function("count", |b| b.iter(|| count(black_box(&counter))));
}

pub fn count(n: &usize) {
    let mut _c = 0;
    for i in 0..*n {
        _c += i;
    }
}
