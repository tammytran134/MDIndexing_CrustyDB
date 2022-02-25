use criterion::Criterion;
use utilities::template::Template;

pub trait BenchTemplate {
    fn bench_server(&mut self, c: &mut Criterion, name: &str);
}

impl BenchTemplate for Template {
    fn bench_server(&mut self, c: &mut Criterion, name: &str) {
        // println!("Running setup for {:?}", name);
        self.run_setup();
        // println!("Starting main benchmark function...");
        c.bench_function(name, |b| b.iter(|| self.run_commands()));
        // println!("Starting main benchmark function...END");
        // println!("Cleaning up {:?}", name);
        // drop trait for template does cleanup automatically, no need to call below
        // self.run_cleanup();
    }
}
