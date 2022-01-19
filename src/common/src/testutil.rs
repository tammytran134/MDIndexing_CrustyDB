use crate::{Attribute, Constraint, DataType, Field, TableSchema, Tuple};
// use csv::Writer;
use crate::table::Table;
use itertools::izip;
use rand::distributions::Alphanumeric;
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng, Rng,
};
use std::env;
use std::path::PathBuf;

pub fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

pub fn gen_uniform_strings(n: u64, cardinality: Option<u64>, min: usize, max: usize) -> Vec<Field> {
    let mut rng = rand::thread_rng();
    let mut ret: Vec<Field> = Vec::new();
    if let Some(card) = cardinality {
        let values: Vec<Field> = (0..card)
            .map(|_| Field::StringField(gen_rand_string_range(min, max)))
            .collect();
        assert_eq!(card as usize, values.len());
        //ret = values.iter().choose_multiple(&mut rng, n as usize).collect();
        let uniform = Uniform::new(0, values.len());
        for _ in 0..n {
            let idx = uniform.sample(&mut rng);
            assert!(idx < card as usize);
            ret.push(values[idx].clone())
        }
        //ret = rng.sample(values, n);
    } else {
        for _ in 0..n {
            ret.push(Field::StringField(gen_rand_string_range(min, max)))
        }
    }
    ret
}

pub fn gen_uniform_ints(n: u64, cardinality: Option<u64>) -> Vec<Field> {
    let mut rng = rand::thread_rng();
    let mut ret = Vec::new();
    if let Some(card) = cardinality {
        if card > i32::MAX as u64 {
            panic!("Cardinality larger than i32 max")
        }
        if n == card {
            // all values distinct
            if n < i32::MAX as u64 / 2 {
                for i in 0..card as i32 {
                    ret.push(Field::IntField(i));
                }
            } else {
                for i in i32::MIN..i32::MIN + (card as i32) {
                    ret.push(Field::IntField(i));
                }
            }
            //ret.shuffle(&mut rng);
        } else {
            let mut range = Uniform::new_inclusive(i32::MIN, i32::MIN + (card as i32) - 1);
            if card < (i32::MAX / 2) as u64 {
                range = Uniform::new_inclusive(0, card as i32 - 1);
            }
            for _ in 0..n {
                ret.push(Field::IntField(range.sample(&mut rng) as i32));
            }
        }
    } else {
        for _ in 0..n {
            ret.push(Field::IntField(rng.gen::<i32>()));
        }
    }
    ret
}

pub fn gen_table_for_test_tuples(table_name: String) -> Table {
    // Building table that matches attributes in gen_test_tuples
    let mut attributes: Vec<Attribute> = Vec::new();
    let pk_attr = Attribute {
        name: String::from("id"),
        dtype: DataType::Int,
        constraint: Constraint::PrimaryKey,
    };
    attributes.push(pk_attr);

    for n in 1..5 {
        let attr = Attribute {
            name: format!("ia{}", n.to_string()),
            dtype: DataType::Int,
            constraint: Constraint::None,
        };
        attributes.push(attr);
    }
    for n in 1..5 {
        let attr = Attribute {
            name: format!("sa{}", n.to_string()),
            dtype: DataType::String,
            constraint: Constraint::None,
        };
        attributes.push(attr);
    }
    let table_schema = TableSchema::new(attributes);

    Table::new(table_name, table_schema)
}

pub fn gen_test_tuples(n: u64) -> Vec<Tuple> {
    let keys = gen_uniform_ints(n, Some(n));
    let i1 = gen_uniform_ints(n, Some(10));
    let i2 = gen_uniform_ints(n, Some(100));
    let i3 = gen_uniform_ints(n, Some(1000));
    let i4 = gen_uniform_ints(n, Some(10000));
    let s1 = gen_uniform_strings(n, Some(10), 10, 20);
    let s2 = gen_uniform_strings(n, Some(100), 10, 20);
    let s3 = gen_uniform_strings(n, Some(1000), 10, 20);
    let s4 = gen_uniform_strings(n, Some(10000), 10, 30);
    let mut tuples = Vec::new();
    for (k, a, b, c, d, e, f, g, h) in izip!(keys, i1, i2, i3, i4, s1, s2, s3, s4) {
        let vals: Vec<Field> = vec![k, a, b, c, d, e, f, g, h];
        tuples.push(Tuple::new(vals));
    }
    tuples
}

/// Converts an int vector to a Tuple.
///
/// # Argument
///
/// * `data` - Data to put into tuple.
pub fn int_vec_to_tuple(data: Vec<i32>) -> Tuple {
    let mut tuple_data = Vec::new();

    for val in data {
        tuple_data.push(Field::IntField(val));
    }

    Tuple::new(tuple_data)
}

/// Creates a Vec of tuples containing IntFields given a 2D Vec of i32 's
pub fn create_tuple_list(tuple_data: Vec<Vec<i32>>) -> Vec<Tuple> {
    let mut tuples = Vec::new();
    for item in &tuple_data {
        let fields = item.iter().map(|i| Field::IntField(*i)).collect();
        tuples.push(Tuple::new(fields));
    }
    tuples
}

/// Creates a new table schema for a table with width number of IntFields.
pub fn get_int_table_schema(width: usize) -> TableSchema {
    let mut attrs = Vec::new();
    for _ in 0..width {
        attrs.push(Attribute::new(String::new(), DataType::Int))
    }
    TableSchema::new(attrs)
}

pub fn get_random_byte_vec(n: usize) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..n).map(|_| rand::random::<u8>()).collect();
    random_bytes
}

pub fn gen_rand_string_range(min: usize, max: usize) -> String {
    if min >= max {
        return gen_rand_string(min);
    }
    let mut rng = rand::thread_rng();
    let size = rng.gen_range(min..max);
    thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}

pub fn gen_rand_string(n: usize) -> String {
    thread_rng()
        .sample_iter(Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

pub fn gen_random_dir() -> PathBuf {
    init();
    let mut dir = env::temp_dir();
    dir.push(String::from("crusty"));
    let rand_string = gen_rand_string(10);
    dir.push(rand_string);
    dir
}

pub fn get_random_vec_of_byte_vec(n: usize, min_size: usize, max_size: usize) -> Vec<Vec<u8>> {
    let mut res: Vec<Vec<u8>> = Vec::new();
    for _ in 0..n {
        res.push((min_size..max_size).map(|_| rand::random::<u8>()).collect());
    }
    res
}

pub fn compare_unordered_byte_vecs(a: &[Vec<u8>], mut b: Vec<Vec<u8>>) -> bool {
    // Quick check
    if a.len() != b.len() {
        return false;
    }
    // check if they are the same ordered
    let non_match_count = a
        .iter()
        .zip(b.iter())
        .filter(|&(j, k)| j[..] != k[..])
        .count();
    if non_match_count == 0 {
        return true;
    }

    // Now check if they are out of order
    for x in a {
        let pos = b.iter().position(|y| y[..] == x[..]);
        match pos {
            None => {
                //Was not found, not equal
                return false;
            }
            Some(idx) => {
                b.swap_remove(idx);
            }
        }
    }
    //since they are the same size, b should be empty
    b.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_tuple_gen() {
        let t = gen_test_tuples(10);
        assert_eq!(10, t.len());
    }

    #[test]
    fn test_uniform_strings() {
        let mut card = 10;
        let mut strs = gen_uniform_strings(100, Some(card), 10, 20);
        let mut map = HashMap::new();

        for x in &strs {
            if let Field::StringField(val) = x {
                assert!(val.len() < 20);
            }
        }
        assert_eq!(100, strs.len());
        for i in strs {
            if let Field::StringField(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card as usize, map.keys().len());

        card = 100;
        map.clear();
        strs = gen_uniform_strings(4800, Some(card), 10, 20);
        for i in strs {
            if let Field::StringField(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card as usize, map.keys().len());
    }

    #[test]
    fn test_uniform_ints() {
        let mut ints = gen_uniform_ints(4, Some(6));
        for x in &ints {
            if let Field::IntField(a) = x {
                assert!(*a < 7);
            }
        }
        let mut card: usize = 20;
        ints = gen_uniform_ints(1000, Some(card as u64));
        assert_eq!(1000, ints.len());

        let mut map = HashMap::new();
        for i in ints {
            if let Field::IntField(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card, map.keys().cloned().count());

        card = 121;
        map.clear();
        ints = gen_uniform_ints(10000, Some(card as u64));
        assert_eq!(10000, ints.len());
        let mut map = HashMap::new();
        for i in ints {
            if let Field::IntField(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card, map.keys().cloned().count());

        card = 500;
        map.clear();
        ints = gen_uniform_ints(card as u64, Some(card as u64));
        let mut map = HashMap::new();
        for i in ints {
            if let Field::IntField(val) = i {
                *map.entry(val).or_insert(0) += 1;
            }
        }
        assert_eq!(card, map.keys().cloned().count());
    }

    /*use rand::seq::SliceRandom;
    use rand::thread_rng;*/
    /*#[test]
    fn test_compare() {
        let mut rng = thread_rng();
        let a = get_random_vec_of_byte_vec(100, 10, 20);
        let b = a.clone();
        assert!(true, compare_unordered_byte_vecs(&a, b));
        let mut b = a.clone();
        b.shuffle(&mut rng);
        assert!(true, compare_unordered_byte_vecs(&a, b));
        let new_rand = get_random_vec_of_byte_vec(99, 10, 20);
        assert!(false, compare_unordered_byte_vecs(&a, new_rand));
        let mut b = a.clone();
        b[rng.gen_range(0..a.len())] = get_random_byte_vec(10);
        assert!(false, compare_unordered_byte_vecs(&a, b));
    }*/
}
