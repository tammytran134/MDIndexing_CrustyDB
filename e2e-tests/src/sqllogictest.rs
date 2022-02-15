#[cfg(test)]
mod test {
    use std::fs;
    use std::path::Path;
    use utilities::sqllogictest_utils::run_sqllogictests_in_file;

    const E2E_DIR: &str = "e2e-tests";
    #[test]
    fn run_sql_logic_tests() {
        let test_dir = if std::env::current_dir().unwrap().ends_with(E2E_DIR) {
            Path::new("testdata").to_path_buf()
        } else {
            Path::new(E2E_DIR).join("testdata")
        };
        assert!(test_dir.exists());
        for entry in fs::read_dir(test_dir).unwrap() {
            let path = entry.unwrap().path();
            assert!(path.exists());
            println!(" *** Running Test {:?} ***", path);
            run_sqllogictests_in_file(&path).unwrap();
        }
    }


    #[test]
    fn run_sql_join() {
        //Sample test to run 1 evaluation plan
        let path = if std::env::current_dir().unwrap().ends_with(E2E_DIR) {
            Path::new("testdata").join("join1")
        } else {
            Path::new(E2E_DIR).join("testdata").join("join1")
        };
        assert!(path.exists());
        let res = run_sqllogictests_in_file(&path);
        let is_ok = res.is_ok();
        match res {
            Ok(()) => {
                assert!(is_ok)
            }
            Err(e) => {
                println!("Error: {:?}", e);
                assert!(is_ok);
            }
        }
    }
}
