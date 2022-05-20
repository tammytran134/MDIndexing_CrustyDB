use crate::benchmarks::benchtemplate::BenchTemplate;
use common::commands::Commands;
use criterion::{criterion_group, Criterion};
use utilities::template::Template;
use rstar::RTree;
use rand::Rng;

const BASE_PATH: &str = "../test_data/";
const KD: usize = 0;
const R: usize = 1;
const data_num: usize = 100;
const k: usize = 5;
const MAX: i64 = 65536;

fn setup_table(bt: &mut Template, table: &str, dim: usize, tree_type: usize) {
    bt.string_to_setup("CREATE TABLE test (a int primary key,b int, c int);".to_string());
    bt.string_to_setup(format!("\\i {}{}.csv test", BASE_PATH, table));
    if dim == 2 {
        if tree_type == KD {
            bt.string_to_setup("\\createIndex KD md_index test (b,c)".to_string());
        }
        else {
            bt.string_to_setup("\\createIndex R md_index test (b,c)".to_string());
        }
    }
    else if dim == 3 {
        if tree_type == KD {
            bt.string_to_setup("\\createIndex KD md_index test (a,b,c)".to_string());
        }
        else {
            bt.string_to_setup("\\createIndex R md_index test (a,b,c)".to_string());
        }
    }
}

fn setup_data_range(bt: &mut Template, dim: usize) -> Vec<String> {
    let mut res = Vec::new();
    let base: i64 = 2;
    let mut rng = rand::thread_rng();
    if dim == 2 {
        for i in 0..data_num {
            let num1 = rng.gen_range(0..MAX);
            let num2 = rng.gen_range(num1+1..MAX);
            let num3 = rng.gen_range(0..MAX);
            let num4 = rng.gen_range(num3+1..MAX);
            res.push(format!("({},{});({},{})", num1.to_string(), num2.to_string(), 
            num3.to_string(), num4.to_string()));
        }
    }
    else {
        for i in 0..data_num {
            let num1 = rng.gen_range(0..MAX);
            let num2 = rng.gen_range(num1+1..MAX);
            let num3 = rng.gen_range(0..MAX);
            let num4 = rng.gen_range(num3+1..MAX);
            let num5 = rng.gen_range(0..MAX);
            let num6 = rng.gen_range(num5+1..MAX);
            res.push(format!("({},{},{});({},{},{})", num1.to_string(), num2.to_string(), 
            num3.to_string(), num4.to_string(), num5.to_string(), num6.to_string()));
        }
    }
    res
}

fn setup_data_knn(bt: &mut Template, dim: usize) -> Vec<String> {
    let mut res = Vec::new();
    let mut rng = rand::thread_rng();
    if dim == 2 {
        for i in 0..data_num {
            res.push(format!("({},{})", rng.gen_range(0..MAX).to_string(),
            rng.gen_range(0..MAX).to_string()));
        }
    }
    else {
        for i in 0..data_num {
            res.push(format!("({},{},{})", rng.gen_range(0..MAX).to_string(),
            rng.gen_range(0..MAX).to_string(),
            rng.gen_range(0..MAX).to_string()));
        }
    }
    res
}

fn bench_index_kd_2d_range_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_range_100");
}

fn bench_index_kd_2d_range_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_range_1000");
}

fn bench_index_kd_2d_range_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_range_10000");
}

fn bench_index_kd_2d_range_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_range_50000");
}

fn bench_index_kd_3d_range_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_range_100");
}

fn bench_index_kd_3d_range_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_range_1000");
}

fn bench_index_kd_3d_range_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_range_10000");
}

fn bench_index_kd_3d_range_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD RANGE md_index test {}", single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_range_50000");
}

fn bench_index_scan_2d_range_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
                                            single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_2d_range_100");
}

fn bench_index_scan_2d_range_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
        single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_2d_range_1000");
}

fn bench_index_scan_2d_range_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
        single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_2d_range_10000");
}

fn bench_index_scan_2d_range_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 2, KD);
    let data = setup_data_range(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
        single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_2d_range_50000");
}

fn bench_index_scan_3d_range_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
        single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_3d_range_100");
}

fn bench_index_scan_3d_range_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
        single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_3d_range_1000");
}

fn bench_index_scan_3d_range_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
        single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_3d_range_10000");
}

fn bench_index_scan_3d_range_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 3, KD);
    let data = setup_data_range(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::ExecuteSQL(format!("\\useIndex R RANGE md_index test {}", 
        single_data))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_scan_3d_range_50000");
}

fn bench_index_kd_2d_knn_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 2, KD);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_knn_100");
}

fn bench_index_kd_2d_knn_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 2, KD);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_knn_1000");
}

fn bench_index_kd_2d_knn_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 2, KD);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_knn_10000");
}

fn bench_index_kd_2d_knn_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 2, KD);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_2d_knn_50000");
}

fn bench_index_kd_3d_knn_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 3, KD);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_knn_100");
}

fn bench_index_kd_3d_knn_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 3, KD);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_knn_1000");
}

fn bench_index_kd_3d_knn_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 3, KD);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_knn_10000");
}

fn bench_index_kd_3d_knn_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 3, KD);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex KD KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_kd_3d_knn_50000");
}

fn bench_index_r_2d_knn_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 2, R);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_2d_knn_100");
}

fn bench_index_r_2d_knn_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 2, R);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_2d_knn_1000");
}

fn bench_index_r_2d_knn_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 2, R);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_2d_knn_10000");
}

fn bench_index_r_2d_knn_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 2, R);
    let data = setup_data_knn(&mut bt, 2);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_2d_knn_50000");
}

fn bench_index_r_3d_knn_100(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index100", 3, R);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_3d_knn_100");
}

fn bench_index_r_3d_knn_1000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index1000", 3, R);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_3d_knn_1000");
}

fn bench_index_r_3d_knn_10000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index10000", 3, R);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_3d_knn_10000");
}

fn bench_index_r_3d_knn_50000(c: &mut Criterion) {
    let mut bt = Template::new();

    setup_table(&mut bt, "index50000", 3, R);
    let data = setup_data_knn(&mut bt, 3);
    bt.add_command(Commands::QuietMode);
    for single_data in data {
        bt.add_command(Commands::UseIndex(format!("\\useIndex R KNN md_index test {} {}", single_data, k))
        );        
    }
    bt.show_configuration();
    bt.bench_server(c, "index_r_3d_knn_50000");
}

criterion_group! {
    name = indexbench;
    config = Criterion::default().sample_size(10);
    targets =
    bench_index_kd_2d_range_100,
    // bench_index_kd_2d_range_1000,
    // bench_index_kd_2d_range_10000,
    // bench_index_kd_2d_range_50000,
    // bench_index_scan_2d_range_100,
    // bench_index_scan_2d_range_1000,
    // bench_index_scan_2d_range_10000,
    // bench_index_scan_2d_range_50000,
    // bench_index_kd_3d_range_100,
    // bench_index_kd_3d_range_1000,
    // bench_index_kd_3d_range_10000,
    // bench_index_kd_3d_range_50000,
    // bench_index_scan_3d_range_100,
    // bench_index_scan_3d_range_1000,
    // bench_index_scan_3d_range_10000,
    // bench_index_scan_3d_range_50000,
    // bench_index_kd_2d_knn_100,
    // bench_index_kd_2d_knn_1000,
    // bench_index_kd_2d_knn_10000,
    // bench_index_kd_2d_knn_50000,
    // bench_index_r_2d_knn_100,
    // bench_index_r_2d_knn_1000,
    // bench_index_r_2d_knn_10000,
    // bench_index_r_2d_knn_50000,
    // bench_index_kd_3d_knn_100,
    // bench_index_kd_3d_knn_1000,
    // bench_index_kd_3d_knn_10000,
    // bench_index_kd_3d_knn_50000,
    // bench_index_r_3d_knn_100,
    // bench_index_r_3d_knn_1000,
    // bench_index_r_3d_knn_10000,
    // bench_index_r_3d_knn_50000,
}

// const test_dim: [usize; 7] = [1, 2, 3, 5, 10, 15, 30];
// const iterations: usize = 100;

// fn csv_to_r_tree(file: &str, dim: usize, n: usize) -> RTree {
//     let mut data = Vec::new();
//     let file_name = format!("{}{}.csv", BASE_PATH, file));
//     let path = fs::canonicalize(file_name)?;
//     debug!("server::csv_utils trying to open file, path: {:?}", path);
//     let file = File::open(path)?;
//     // Create csv reader.
//     let mut rdr = csv::ReaderBuilder::new()
//         .has_headers(false)
//         .from_reader(file);
//     let i = 0;
//     for result in rdr.records() {
//         #[allow(clippy::single_match)]
//         match result {
//             Ok(rec) => {
//                 let mut tuple = Vec::new();
//                 let j = 0;
//                 for field in rec.iter() {
//                     let value: i32 = field.parse::<i32>().unwrap();
//                     tuple.push(value);
//                     j += 1;
//                     if j == dim {
//                         break;
//                     }
//                 }
//                 data.push([tuple[..]]);
//             }
//             _ => {
//                 // FIXME: get error from csv reader
//                 error!("Could not read row from CSV");
//             }
//         }
//         i += 1;
//         if i == n {
//             break;
//         }
//     }
//     tree = RTree::bulk_load(data.clone());
//     tree
// }

