use common::{CrustyError, Tuple};

use std::fs::OpenOptions;
use std::io::Write;

/// A utility function to take a list of tuples and writes them to a new CSV file at the path
pub fn write_tuples_to_new_csv(
    file_name: String,
    tuples: Vec<Tuple>,
) -> Result<String, CrustyError> {
    let csv = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(file_name);
    match csv {
        Ok(mut f) => {
            for t in tuples {
                writeln!(&mut f, "{}", t.to_csv())?;
            }
        }
        Err(e) => {
            return Err(CrustyError::CrustyError(e.to_string()));
        }
    }
    Ok("ok".to_string())
}

// #[cfg(test)]
// mod libtests {
//     use crate::csv_utils::{write_tuples_to_new_csv};
//     use crate::StorageManager;
//     use common::ids::ContainerId;
//     use common::ids::StateType;
//     use common::storage_trait::StorageTrait;
//     use common::testutil::gen_table_for_test_tuples;
//     use common::{testutil, Tuple};

//     use std::fs;
//     use std::sync::{Arc, RwLock};
//     use txn_manager::transactions::Transaction;

//     /// Create csv from tuples and checks if csv's are readable
//     #[test]
//     fn test_generate_temp_csv() {
//         // num of records in csv generated later
//         let n = 100;
//         let table_name = String::from("table1");
//         let container_id: ContainerId = 0;

//         // Generate random temp dir path then create it
//         info!("Generating temp directory");
//         let temp_dir = testutil::gen_random_dir();
//         fs::create_dir_all(temp_dir.clone()).unwrap();
//         let temp_dir_string = temp_dir.into_os_string().into_string();

//         // Generate test.csv and grab a single tuple to search for later
//         let test_tuple: Tuple;
//         let path: String;
//         // Generate csv's into random temp dir
//         if let Ok(str_path) = temp_dir_string.clone() {
//             let tuples = testutil::gen_test_tuples(n);
//             test_tuple = tuples.get(0).unwrap().clone();
//             let _ = write_tuples_to_new_csv(str_path.clone() + "/test.csv", tuples);
//             path = str_path + "/test.csv";
//         } else {
//             error!("Unable to insert tuples into csv");
//             panic!();
//         }

//         let table = gen_table_for_test_tuples(table_name.clone());
//         let txn = Transaction::new();
//         let dsm = StorageManager::new_test_sm();
//         dsm.create_container(
//             container_id,
//             StorageManager::get_simple_config(),
//             Some(table_name.clone()),
//             StateType::BaseTable,
//             None,
//         )
//         .unwrap();
//         let time = 0;

//         let result = dsm.import_csv(&table, path, txn.tid(), container_id, time);

//         match result {
//             Ok(_) => {
//                 // Prepare seqscan opiterator
//                 let dsm_arg = Box::new(dsm);
//                 let sm: &'static StorageManager = Box::leak(dsm_arg);
//                 let table_arg = Arc::new(RwLock::new(table));
//                 let mut delta_iterator =
//                     SeqScan::new(sm, table_arg, &table_name, &container_id, time);
//                 let _ = delta_iterator.open();

//                 // Iteration algorithm
//                 let mut found_tuple: bool = false;
//                 let mut next_result = delta_iterator.next();
//                 while let Ok(Some(next_tuple)) = next_result {
//                     if next_tuple == test_tuple {
//                         info!("{}", next_tuple.to_string());
//                         found_tuple = true;
//                         break;
//                     }

//                     next_result = delta_iterator.next();
//                 }
//                 assert!(found_tuple)
//             }
//             Err(s) => {
//                 error!("{}", s.to_string());
//             }
//         }

//         // TODO: Delete temp dir
//         println!("Deleting temp directory");
//         fs::remove_dir_all(temp_dir_string.unwrap()).unwrap();
//     }
// }
