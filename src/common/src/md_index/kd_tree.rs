use std::cmp::Ordering::{Less, Greater, Equal};
use std::any::type_name;
use crate::Tuple;
use crate::Field;
use crate::Field::{IntField, StringField};

#[derive(Clone, PartialEq, PartialOrd)]
pub struct KdTree {
    pub dim: usize,
    pub arr: Vec<Option<Vec<Field>>>,
    pub idx_fields: Vec<usize>,
    pub total_dim: usize,
}


impl KdTree {
    pub fn new(dim: usize, idx_fields: Vec<usize>, total_dim: usize) -> Self {
        Self {
            dim,
            arr: Vec::new(),
            idx_fields,
            total_dim,
        }
    }

    fn padding(&self, val: &Vec<Field>) -> Vec<Field> {
        if val.len() == self.total_dim {
            return val.clone();
        }
        let mut res = Vec::new();
        let mut curr_padded_idx = 0;
        let mut curr_idx = 0;
        for idx in &self.idx_fields {
            while curr_padded_idx < *idx {
                res.push(IntField(0));
                curr_padded_idx += 1;
            }
            res.push(val[curr_idx].clone());
            curr_idx += 1;
            curr_padded_idx += 1;
            continue;
        }
        res
    }

    fn compare_val(&self, val1: &Vec<Field>, val2: &Vec<Field>) -> bool {
        if val1.len() == 0 {
            if val2.len() == 0 {
                return true
            }
            else {
                return false
            }
        }
        else {
            for i in 0..self.idx_fields.len() {
                match (&val1[self.idx_fields[i]], &val2[self.idx_fields[i]]) {    
                    (IntField(x), IntField(y)) => {
                        if x != y {
                            return false
                        }
                    },                        
                    (StringField(x), StringField(y)) => {
                        if !x.eq(y) {
                            return false
                        }
                    }, 
                    (_, _) => {
                        return false
                    }
                }  
            }
            return true
        }
    }


    fn compare_val_at_dim(&self, val1: &Vec<Field>, val2: &Vec<Field>, dim: usize) -> i8 {
        let curr_idx = self.idx_fields[dim];
        match (&val1[curr_idx], &val2[curr_idx]) {    
            (IntField(x), IntField(y)) => {
                if x < y {
                    return -1
                }
                else if x == y {
                    return 0
                }
                else {
                    return 1
                }
            },                    
            (StringField(x), StringField(y)) => {
                match x.cmp(&y) {
                    Less => -1,
                    Equal => 0,
                    Greater => 1,
                }
            }, 
            (_, _) => {
                return 0
            }
        }             
    }

    fn get_val(&self, val: &Vec<Field>) -> Vec<Field> {
        let mut res = Vec::new();
        for i in 0..self.total_dim {
            match &val[i] {
                IntField(x) => {res.push(IntField(*x));}
                StringField(x) => {res.push(StringField(x.to_string()));}
            }
        }
        return res;
    }

    fn print_val2(&self, val: &Vec<Field>) {
        if val.len() == 0 {
            println!("Empty val");
        }
        println!("[");
        for element in val {
            match element {
                IntField(x) => {println!("{},", x)},
                StringField(x) => {println!("{},", x)},            
            }
        }
        println!("]\n");
    }

    fn insert_helper(&mut self, val: &Vec<Field>, node_idx: usize, depth: usize) {
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx {
            for i in arr_len..node_idx {
                self.arr.push(None);
            }
            self.arr.push(Some(self.get_val(val)));
            return
        }
        if self.arr[node_idx].is_none() {
            self.arr[node_idx] = Some(self.get_val(val));
            return
        }
        let curr_dim = depth % self.dim;
        if self.compare_val_at_dim(&val, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
            self.insert_helper(val, node_idx*2+1, depth + 1)
        }
        else {
            self.insert_helper(val, node_idx*2+2, depth + 1)
        }
    }

    pub fn insert(&mut self, val: &Vec<Field>) {
        self.insert_helper(val, 0, 0)
    } 

    fn search_helper(&mut self, val: &Vec<Field>, node_idx: usize, depth: usize) -> bool {
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx {
            return false
        }
        if self.arr[node_idx].is_none() {
            return false
        }
        if self.compare_val(&self.arr[node_idx].as_ref().unwrap(), val) {
            return true
        }
        let curr_dim = depth % self.dim;
        if self.compare_val_at_dim(val, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
            self.search_helper(val, node_idx*2+1, depth + 1)
        }
        else {
            self.search_helper(val, node_idx*2+2, depth + 1)
        }
    }

    pub fn search(&mut self, val: &Vec<Field>) -> bool {
        self.search_helper(&self.padding(val), 0, 0)
    }

    fn get_helper(&mut self, val: &Vec<Field>, node_idx: usize, depth: usize, res: &mut Vec<Vec<Field>>) {
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx {
            return
        }
        if self.arr[node_idx].is_none() {
            return
        }
        if self.compare_val(&self.arr[node_idx].as_ref().unwrap(), val) {
            res.push(self.arr[node_idx].as_ref().unwrap().clone());
        }
        let curr_dim = depth % self.dim;
        if self.compare_val_at_dim(val, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
            self.get_helper(val, node_idx*2+1, depth + 1, res)
        }
        else {
            self.get_helper(val, node_idx*2+2, depth + 1, res)
        }
    }

    pub fn get(&mut self, val: &Vec<Field>) -> Vec<Vec<Field>> {
        let mut res = Vec::new();
        self.get_helper(&self.padding(val), 0, 0, &mut res);
        res
    }

    fn if_smaller(&self, val1: &Vec<Field>, val2: &Vec<Field>) -> bool {
        if val1.len() == 0 {
            if val2.len() == 0 {
                return true
            }
            else {
                return false
            }
        }
        else {
            for i in 0..self.idx_fields.len() {
                match (&val1[self.idx_fields[i]], &val2[self.idx_fields[i]]) {    
                    (IntField(x), IntField(y)) => {
                        if x > y {
                            return false
                        }
                    },                        
                    (StringField(x), StringField(y)) => {
                        if let Greater = x.cmp(y) {
                            return false
                        }
                    }, 
                    (_, _) => {
                        return false
                    }
                }  
            }
            return true
        }
    }

    fn if_greater(&self, val1: &Vec<Field>, val2: &Vec<Field>) -> bool {
        if val1.len() == 0 {
            if val2.len() == 0 {
                return true
            }
            else {
                return false
            }
        }
        else {
            for i in 0..self.idx_fields.len() {
                match (&val1[self.idx_fields[i]], &val2[self.idx_fields[i]]) {    
                    (IntField(x), IntField(y)) => {
                        if x < y {
                            return false
                        }
                    },                        
                    (StringField(x), StringField(y)) => {
                        if let Less = x.cmp(y) {
                            return false
                        }
                    }, 
                    (_, _) => {
                        return false
                    }
                }  
            }
            return true
        }
    }

    fn if_within_range(&self, val: &Vec<Field>, min: &Vec<Field>, max: &Vec<Field>) -> bool {
        self.if_smaller(val, max) && self.if_greater(val, min)
    }

    fn range_query_helper(&mut self, min:&Vec<Field>, max: &Vec<Field>, node_idx: usize, depth: usize, res: &mut Vec<Vec<Field>>) {
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx {
            return
        }
        if self.arr[node_idx].is_none() {
            return
        }
        if self.if_within_range(&self.arr[node_idx].as_ref().unwrap(), min, max) {
            res.push(self.arr[node_idx].as_ref().unwrap().clone());
        }
        let curr_dim = depth % self.dim;
        if self.compare_val_at_dim(min, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
            self.range_query_helper(min, max, node_idx*2+1, depth + 1, res)
        }
        if self.compare_val_at_dim(&self.arr[node_idx].as_ref().unwrap(), max, curr_dim) < 0 {
            self.range_query_helper(min, max, node_idx*2+2, depth + 1, res)
        }        
    }

    pub fn range_query(&mut self, min:&Vec<Field>, max: &Vec<Field>) -> Vec<Vec<Field>> {
        let mut res = Vec::new();
        self.range_query_helper(&self.padding(min), &self.padding(max), 0, 0, &mut res);
        res
    }

    fn copy_from_vec(&self, array: &mut [Vec<Field>], vec: &Vec<Vec<Field>>) {
        if array.len() == 0 {
            return
        }
        if array.len() < vec.len() {
        }
        if array.len() > vec.len() {
        }
        for i in 0..array.len() {
            array[i] = self.get_val(&vec[i]);
        }
    }


    fn print_arr(&self, array: &[Vec<Field>]) {
        let arr_len = array.len();
        if arr_len == 0 {
            println!("Empty arr\n");
        }  
        println!("[");
        for element in array {
            println!("[");
            for single_val in element {
                match single_val {
                    IntField(x) => {println!("{},", x)},
                    StringField(x) => {println!("{},", x)},           
                }
            }
            println!("], ");
        }  
        println!("]\n");
    }

    pub fn print_vec(vec: &Vec<Vec<Field>>) {
        let vec_len = vec.len();
        if vec_len == 0 {
            debug!("Empty vec\n");
        }  
        debug!("[");
        for element in vec {
            debug!("[");
            for single_val in element {
                match single_val {
                    IntField(x) => {debug!("{},", x)},
                    StringField(x) => {debug!("{},", x)},            
                }
            }
            debug!("], ");
        }  
        debug!("]\n");
    }
    
    fn sort(&self, array: &mut [Vec<Field>], curr_dim: usize) {
        let middle = array.len() / 2;
        if array.len() < 2 {
          return // No need to sort vectors with one element
        }
      
        let mut sorted = array.to_vec();
      
        self.sort(&mut array[..middle], curr_dim);
        self.sort(&mut array[middle..], curr_dim);
        self.merge(&array[..middle], &array[middle..], &mut sorted, curr_dim);
        self.copy_from_vec(array, &sorted); // Copy the sorted result into original vector
      }
      
      fn merge(&self,l_arr: &[Vec<Field>], r_arr: &[Vec<Field>], sorted: &mut [Vec<Field>], curr_dim: usize) {
        // Current loop position in left half, right half, and sorted vector
        let (mut left, mut right, mut i) = (0, 0, 0);
        while left < l_arr.len() && right < r_arr.len() {
          if self.compare_val_at_dim(&l_arr[left], &r_arr[right], curr_dim) <= 0 {
            sorted[i] = l_arr[left].clone();
            i += 1;
            left += 1;
          } else {
            sorted[i] = r_arr[right].clone();
            i += 1;
            right += 1;
          }
        }
      
        if left < l_arr.len() {
          // If there is anything left in the left half append it after sorted members
          self.copy_from_vec(&mut sorted[i..], &l_arr[left..].to_vec());
        }
      
        if right < r_arr.len() {
          // If there is anything left in the right half append it after sorted members
          self.copy_from_vec(&mut sorted[i..], &r_arr[right..].to_vec());
        }
      }

    fn data_into_tree_helper(&mut self, arr: &mut [Vec<Field>], depth: usize) {
        let len_arr = arr.len();
        if len_arr == 0 {
            return
        }
        let curr_dim = depth % self.dim;
        self.sort(arr, curr_dim);
        let median: usize = len_arr/2;
        self.insert(&arr[median]);
        self.data_into_tree_helper(&mut arr[..median], depth + 1);
        self.data_into_tree_helper(&mut arr[median+1..], depth + 1);
    }

    pub fn data_into_tree(&mut self, arr: &mut [Vec<Field>]) {
        let len_arr = arr.len();
        if len_arr == 0 {
            return
        }
        self.data_into_tree_helper(arr, 0);
    }

    fn cmp_min_node(&self, val1: usize, val2: Option<usize>, val3: Option<usize>, curr_dim: usize) -> Option<usize> {
        let mut res = val1;
        let arr_len = self.arr.len();
        if val2.is_some() && arr_len > val2.unwrap() && self.arr[val2.unwrap()].is_some() &&
        self.compare_val_at_dim(&self.arr[val2.unwrap()].as_ref().unwrap(), &self.arr[val1].as_ref().unwrap(), curr_dim) < 0 {
            res = val2.unwrap();
        }
        if val3.is_some() && arr_len > val3.unwrap() && self.arr[val3.unwrap()].is_some() &&
        self.compare_val_at_dim(&self.arr[val3.unwrap()].as_ref().unwrap(), &self.arr[val1].as_ref().unwrap(), curr_dim) < 0 {
            res = val3.unwrap();
        }
        //println!("res is {}", res);
        return Some(res);
    }

    fn find_min_helper(&self, node_idx: usize, curr_dim: usize, depth: usize) -> Option<usize> {
        let arr_len = self.arr.len();
        //println!("node idx to find min is {}", node_idx);
        if arr_len == 0 || arr_len <= node_idx || self.arr[node_idx].is_none() {
            //println!("errorrrrr {}", node_idx);
            return None
        }
        let curr_dim2 = depth % self.dim;
        if curr_dim2 == curr_dim {
            if arr_len < (node_idx*2 + 1) || self.arr[node_idx*2 + 1].is_none() {
                return Some(node_idx);
            }
            else {
                return self.find_min_helper(node_idx*2 + 1, curr_dim, depth + 1);
            }
        }
        return self.cmp_min_node(node_idx, self.find_min_helper(node_idx*2 + 1, curr_dim, depth + 1),
        self.find_min_helper(node_idx*2 + 2, curr_dim, depth + 1), curr_dim);
    }

    fn find_min(&self, node_idx: usize, curr_dim: usize, depth: usize) -> Option<usize> {
        self.find_min_helper(node_idx, curr_dim, depth)
    }

    fn copy_single_val(&self, val: &Field) -> Field {
        match val {
            IntField(x) => {IntField(*x)},
            StringField(x) => {StringField(x.to_string())},
        }        
    }

    fn get_new_copy(&self, node_idx: usize) -> Vec<Field> {
        let mut new_copy = Vec::new();
        let copy_val = self.arr[node_idx].as_ref().unwrap();
        for i in 0..self.total_dim {
            new_copy.push(self.copy_single_val(&copy_val[i]));
        }
        return new_copy
    }

    fn copy_and_delete(&mut self, old_node_idx: usize, new_node_idx: usize) {
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= old_node_idx || self.arr[old_node_idx].is_none() {
            return
        }
        for i in self.arr.len()..(new_node_idx + 1) {
            self.arr.push(None);
        }
        self.arr[new_node_idx] = Some(self.get_new_copy(old_node_idx));
        self.arr[old_node_idx] = None;
        if (arr_len > old_node_idx*2 + 1) && self.arr[old_node_idx*2 + 1].is_some() {
            self.copy_and_delete(old_node_idx*2 + 1, new_node_idx*2 + 1);
        } 
        if (arr_len > old_node_idx*2 + 2) && self.arr[old_node_idx*2 + 2].is_some() {
            self.copy_and_delete(old_node_idx*2 + 2, new_node_idx*2 + 2);
        }
    }

    fn convert_left_to_right_tree(&mut self, node_idx: usize) {
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx || self.arr[node_idx].is_none() {
            //println!("nothing to convert");
            return
        }
        let direction = if node_idx % 2 == 0 {2} else {1};
        let par_node_idx = (node_idx - direction)/2;
        let new_node_idx = par_node_idx*2 + 2;
        for i in arr_len..(new_node_idx + 1) {
            self.arr.push(None);
        }
        if ((arr_len > node_idx*2 + 1) && self.arr[node_idx*2 + 1].is_some()) || 
        ((arr_len > node_idx*2 + 2) && self.arr[node_idx*2 + 2].is_some()) {
            self.copy_and_delete(node_idx*2 + 1, new_node_idx*2 + 1);
            self.copy_and_delete(node_idx*2 + 2, new_node_idx*2 + 2);
        }
        self.arr[new_node_idx] = Some(self.get_new_copy(node_idx));
        self.arr[node_idx] = None;
    }

    fn delete_helper(&mut self, val: &Vec<Field>, node_idx: usize, depth: usize) {
        //println!("node idx is {}", node_idx);
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx || self.arr[node_idx].is_none() {
            return
        }
        let curr_dim = depth % self.dim;
        if self.compare_val(&self.arr[node_idx].as_ref().unwrap(), val) {
            //println!("found");
            if arr_len > (node_idx * 2 + 2) && self.arr[node_idx*2 + 2].is_some() {
                //println!("turn right");
                let min_node_idx = self.find_min(node_idx * 2 + 2, curr_dim, depth + 1);
                if min_node_idx.is_none() {
                    //println!("comes here right");
                    return
                }
                //println!("min node index right is {}", min_node_idx.unwrap());
                self.arr[node_idx] = Some(self.get_new_copy(min_node_idx.unwrap()));
                let new_val = self.get_new_copy(min_node_idx.unwrap());
                self.delete_helper(&new_val, node_idx*2 + 2, depth + 1);        
            }
            else if arr_len > (node_idx * 2 + 1) && self.arr[node_idx*2 + 1].is_some() {
                //println!("turn left");
                let mut min_node_idx = self.find_min(node_idx * 2 + 1, curr_dim, depth + 1);
                if min_node_idx.is_none() {
                    //println!("comes here left");
                    return
                }
                //println!("min node index left is {}", min_node_idx.unwrap());
                self.arr[node_idx] = Some(self.get_new_copy(min_node_idx.unwrap()));
                let new_val = self.get_new_copy(min_node_idx.unwrap());
                self.delete_helper(&new_val, node_idx*2 + 1, depth + 1);
                self.convert_left_to_right_tree(node_idx*2 + 1);                      
            }
            else {
                //println!("reach end of tree");
                self.arr[node_idx] = None;
                return
            }
            return
        }
        else {
            if self.compare_val_at_dim(val, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
                //println!("turn left");
                self.delete_helper(val, node_idx * 2 + 1, depth + 1)
            }
            else {
                //println!("turn right");
                self.delete_helper(val, node_idx * 2 + 2, depth + 1)
            }
        }
    }

    pub fn delete(&mut self, val: &Vec<Field>) {
        self.delete_helper(val, 0, 0)
    }

    fn print_val(val: &Option<Vec<Field>>) {
        if val.is_none() {
            debug!("Empty tree");
        }
        let val_arr = val.as_ref().unwrap();
        debug!("[");
        for element in val_arr {
            match element {
                IntField(x) => {debug!("{},", x)},
                StringField(x) => {debug!("{},", x)},          
            }
        }
        debug!("]\n");
    }

    fn print_tree_helper(&self, node_idx: usize, depth: usize) {
        debug!("depth level: {}\n", depth);
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx {
            return 
        }
        KdTree::print_val(&self.arr[node_idx]);
        if arr_len > (node_idx*2 + 1) && self.arr[node_idx*2 + 1].is_some() {
            debug!("left\n");
            self.print_tree_helper(node_idx*2 + 1, depth + 1);
        }
        if arr_len > (node_idx*2 + 2) && self.arr[node_idx*2 + 2].is_some() {
            debug!("right\n");
            self.print_tree_helper(node_idx*2 + 2, depth + 1);
        }
    }

    pub fn print_tree(&self) {
        self.print_tree_helper(0, 0)
    }
    
    pub fn int_val_to_field(vec: &Vec<i32>) -> Vec<Field> {
        let mut res: Vec<Field> = Vec::new();
        for element in vec {
            res.push(IntField(*element));
        }
        return res;
    }

    pub fn str_val_to_field(vec: &Vec<String>) -> Vec<Field> {
        let mut res: Vec<Field> = Vec::new();
        for element in vec {
            res.push(StringField(element.to_string()));
        }
        return res;
    }

    pub fn int_arr_to_field<'a>(vec: &'a Vec<Vec<i32>>, res: &'a mut Vec<Vec<Field>>) -> &'a mut [Vec<Field>]  {
        for element in vec {
            res.push(KdTree::int_val_to_field(element));
        }
        return &mut res[..];
    }

    pub fn str_arr_to_field<'a>(vec: &'a Vec<Vec<String>>, res: &'a mut Vec<Vec<Field>>) -> &'a mut [Vec<Field>]  {
        for element in vec {
            res.push(KdTree::str_val_to_field(element));
        }
        return &mut res[..];
    }

    pub fn vec_field_to_tuple(field_vals: &Vec<Vec<Field>>) -> Vec<Tuple> {
        let mut res_tuple = Vec::new();
        for tuple in field_vals {
            res_tuple.push(Tuple::new(tuple.clone()));
        }
        res_tuple
    }

    pub fn print_tuple(&self, tuple: &Tuple) {
        let mut res_str: String = "[".to_owned();
        for single_field in &tuple.field_vals {
            match single_field {
                Field::IntField(x) => {res_str.push_str(&*x.to_string().to_owned())},
                Field::StringField(x) => {res_str.push_str(&x.to_owned())},
            }
        }
        res_str.push_str("]");
        debug!("{}", res_str);
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::testutil::*;

    pub fn tree1() -> KdTree {
        let a: Vec<Field> = vec![IntField(4), IntField(7)];
        let b: Vec<Field> = vec![IntField(3), IntField(8)];
        let c: Vec<Field> = vec![IntField(5), IntField(2)];
        let d: Vec<Field> = vec![IntField(5), IntField(6)];
        let e: Vec<Field> = vec![IntField(2), IntField(9)];
        let f: Vec<Field> = vec![IntField(10), IntField(1)];
        let g: Vec<Field> = vec![IntField(11), IntField(3)];
        let mut tree = KdTree::new(2, vec![0, 1], 2);
        tree.insert(&a);
        tree.insert(&b);
        tree.insert(&c);
        tree.insert(&d);
        tree.insert(&e);
        tree.insert(&f);
        tree.insert(&g);
        tree
    }

    pub fn tree2() -> KdTree {
        let h: Vec<Field> = vec![IntField(5), IntField(6)];
        let i: Vec<Field> = vec![IntField(4), IntField(10)];
        let j: Vec<Field> = vec![IntField(4), IntField(20)];
        let k: Vec<Field> =  vec![IntField(4), IntField(30)];
        let mut tree = KdTree::new(2, vec![0, 1], 2);
        tree.insert(&h);
        tree.insert(&i);
        tree.insert(&j);
        tree.insert(&k);
        tree
    }

    pub fn tree3() -> KdTree {
        let l: Vec<Field> =  vec![IntField(30), IntField(40)];
        let m: Vec<Field> =  vec![IntField(5), IntField(25)];
        let n: Vec<Field> =  vec![IntField(70), IntField(70)];
        let o: Vec<Field> =  vec![IntField(10), IntField(12)];
        let p: Vec<Field> =  vec![IntField(50), IntField(30)];
        let q: Vec<Field> =  vec![IntField(35), IntField(45)];
        let mut tree = KdTree::new(2, vec![0, 1], 2);
        let mut tree_arr: [&Vec<Field>; 6] = [&l, &m, &n, &o, &p, &q];
        for element in tree_arr {
            tree.insert(element);
        }
        tree 
    }

    pub fn tree4() -> KdTree {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(8), IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(10), IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(9), IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(19), IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(20), IntField(3), IntField(6)];
        let mut tree = KdTree::new(3, vec![0, 1, 2], 3);
        tree.insert(&a);
        tree.insert(&b);
        tree.insert(&c);
        tree.insert(&d);
        tree.insert(&e);
        tree.insert(&f);
        tree.insert(&g);
        tree.insert(&h);
        tree.insert(&i);
        tree.insert(&j);
        tree.insert(&k);
        tree
    }

    pub fn tree5() -> KdTree {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(8), IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(10), IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(9), IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(19), IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(20), IntField(3), IntField(6)];
        let mut tree5 = KdTree::new(2, vec![1, 2], 3);
        let mut tree_arr_5: [Vec<Field>; 11] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), 
        f.clone(), g.clone(), h.clone(), i.clone(), j.clone(), k.clone()];
        tree5.data_into_tree(&mut tree_arr_5);
        tree5
    }

    #[test]
    pub fn test_int_arr_to_field() {
        let a = vec![4,7];
        let b = vec![3,8];
        let c = vec![5,2];
        let d = vec![5,6];
        let e = vec![2,9];
        let f = vec![10,1];
        let g = vec![11,3]; 
        let a_field: Vec<Field> = vec![IntField(4), IntField(7)];
        let b_field: Vec<Field> = vec![IntField(3), IntField(8)];
        let c_field: Vec<Field> = vec![IntField(5), IntField(2)];
        let d_field: Vec<Field> = vec![IntField(5), IntField(6)];
        let e_field: Vec<Field> = vec![IntField(2), IntField(9)];
        let f_field: Vec<Field> = vec![IntField(10), IntField(1)];
        let g_field: Vec<Field> = vec![IntField(11), IntField(3)];
        let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
        let mut res = Vec::new();
        let tree_vec_converted = KdTree::int_arr_to_field(&tree_vec, &mut res);
        assert!(tree_vec_converted == &[a_field.clone(), b_field.clone(), c_field.clone(), 
        d_field.clone(), e_field.clone(), f_field.clone(), g_field.clone()]);
    }


    #[test]
    pub fn test_search_tree1() {
        let a: Vec<Field> = vec![IntField(4), IntField(7)];
        let b: Vec<Field> = vec![IntField(3), IntField(8)];
        let c: Vec<Field> = vec![IntField(5), IntField(2)];
        let d: Vec<Field> = vec![IntField(5), IntField(6)];
        let e: Vec<Field> = vec![IntField(2), IntField(9)];
        let f: Vec<Field> = vec![IntField(10), IntField(1)];
        let g: Vec<Field> = vec![IntField(11), IntField(3)];
        let mut tree_1 = tree1();
        assert!(true == tree_1.search(&a));
        assert!(true == tree_1.search(&b));
        assert!(true == tree_1.search(&c));
        assert!(true == tree_1.search(&d));
        assert!(true == tree_1.search(&e));
        assert!(true == tree_1.search(&f));
        assert!(true == tree_1.search(&g));   
    }

    #[test]
    pub fn test_insert_tree1() {
        let a: Vec<Field> = vec![IntField(4), IntField(7)];
        let b: Vec<Field> = vec![IntField(3), IntField(8)];
        let c: Vec<Field> = vec![IntField(5), IntField(2)];
        let d: Vec<Field> = vec![IntField(5), IntField(6)];
        let e: Vec<Field> = vec![IntField(2), IntField(9)];
        let f: Vec<Field> = vec![IntField(10), IntField(1)];
        let g: Vec<Field> = vec![IntField(11), IntField(3)];
        let tree_1 = tree1();
        assert!(tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(a.clone()), Some(b.clone()), Some(c.clone()), None,
            Some(e.clone()), Some(f.clone()), Some(d.clone()), None, None, None,
            None, None, None, None, Some(g.clone())],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });     
    }

    #[test]
    pub fn test_bulk_load_tree1 () {
        let a: Vec<Field> = vec![IntField(4), IntField(7)];
        let b: Vec<Field> = vec![IntField(3), IntField(8)];
        let c: Vec<Field> = vec![IntField(5), IntField(2)];
        let d: Vec<Field> = vec![IntField(5), IntField(6)];
        let e: Vec<Field> = vec![IntField(2), IntField(9)];
        let f: Vec<Field> = vec![IntField(10), IntField(1)];
        let g: Vec<Field> = vec![IntField(11), IntField(3)];
        let mut bulk_load_tree_1 = KdTree::new(2, vec![0, 1], 2);
        let mut tree_arr_1: [Vec<Field>; 7] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
        bulk_load_tree_1.data_into_tree(&mut tree_arr_1);
        assert!(bulk_load_tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(c.clone()), Some(b.clone()), Some(g.clone()), Some(a.clone()),
            Some(e.clone()), Some(f.clone()), Some(d.clone())],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });
    }

    #[test]
    pub fn test_delete_tree1() {
        let a: Vec<Field> = vec![IntField(4), IntField(7)];
        let b: Vec<Field> = vec![IntField(3), IntField(8)];
        let c: Vec<Field> = vec![IntField(5), IntField(2)];
        let d: Vec<Field> = vec![IntField(5), IntField(6)];
        let e: Vec<Field> = vec![IntField(2), IntField(9)];
        let f: Vec<Field> = vec![IntField(10), IntField(1)];
        let g: Vec<Field> = vec![IntField(11), IntField(3)];
        let mut tree_1 = tree1();
        tree_1.delete(&g);
        assert!(tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(a.clone()), Some(b.clone()), Some(c.clone()), None,
            Some(e.clone()), Some(f.clone()), Some(d.clone()), None, None, None,
            None, None, None, None, None,],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });           
        tree_1.delete(&a);
        assert!(tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(c.clone()), Some(b.clone()), Some(d.clone()), None,
            Some(e.clone()), Some(f.clone()), None, None, None, None,
            None, None, None, None, None],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });                 
    }

    #[test]
    pub fn test_search_tree2() {
        let h: Vec<Field> = vec![IntField(5), IntField(6)];
        let i: Vec<Field> = vec![IntField(4), IntField(10)];
        let j: Vec<Field> = vec![IntField(4), IntField(20)];
        let k: Vec<Field> =  vec![IntField(4), IntField(30)];
        let mut tree_2 = tree2();
        assert!(true == tree_2.search(&h));
        assert!(true == tree_2.search(&i));
        assert!(true == tree_2.search(&j));
        assert!(true == tree_2.search(&k));       
    }

    #[test]
    pub fn test_insert_tree2() {
        let h: Vec<Field> = vec![IntField(5), IntField(6)];
        let i: Vec<Field> = vec![IntField(4), IntField(10)];
        let j: Vec<Field> = vec![IntField(4), IntField(20)];
        let k: Vec<Field> =  vec![IntField(4), IntField(30)];
        let tree_2 = tree2();
        assert!(tree_2 == KdTree {
            dim: 2,
            arr: vec![Some(h.clone()), Some(i.clone()), None, None,
            Some(j.clone()), None, None, None, None, None,
            Some(k.clone()),],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });                   
    }

    #[test]
    pub fn test_bulk_load_tree2 () {
        let h: Vec<Field> = vec![IntField(5), IntField(6)];
        let i: Vec<Field> = vec![IntField(4), IntField(10)];
        let j: Vec<Field> = vec![IntField(4), IntField(20)];
        let k: Vec<Field> =  vec![IntField(4), IntField(30)];
        let mut bulk_load_tree_2 = KdTree::new(2, vec![0, 1], 2);
        let mut tree_arr_2: [Vec<Field>; 4] = [h.clone(), i.clone(), j.clone(), k.clone(),];
        bulk_load_tree_2.data_into_tree(&mut tree_arr_2);
        assert!(bulk_load_tree_2 == KdTree {
            dim: 2,
            arr: vec![Some(k.clone()), None, Some(j.clone()), None,
            None, Some(i.clone()), None, None, None, None, None,
            None, Some(h.clone()), ],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });
    }

    #[test]
    pub fn test_delete_tree2() {
        let h: Vec<Field> = vec![IntField(5), IntField(6)];
        let i: Vec<Field> = vec![IntField(4), IntField(10)];
        let j: Vec<Field> = vec![IntField(4), IntField(20)];
        let k: Vec<Field> =  vec![IntField(4), IntField(30)];
        let mut tree_2 = tree2();
        tree_2.delete(&h);
        assert!(tree_2 == KdTree {
            dim: 2,
            arr: vec![Some(i.clone()), None, Some(j.clone()), None,
            None, None, Some(k.clone()), None, None, None, None,],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });
    }

    #[test]
    pub fn test_search_tree3() {
        let l: Vec<Field> =  vec![IntField(30), IntField(40)];
        let m: Vec<Field> =  vec![IntField(5), IntField(25)];
        let n: Vec<Field> =  vec![IntField(70), IntField(70)];
        let o: Vec<Field> =  vec![IntField(10), IntField(12)];
        let p: Vec<Field> =  vec![IntField(50), IntField(30)];
        let q: Vec<Field> =  vec![IntField(35), IntField(45)];
        let mut tree_3 = tree3();
        assert!(true == tree_3.search(&l));
        assert!(true == tree_3.search(&m));
        assert!(true == tree_3.search(&n));
        assert!(true == tree_3.search(&o));  
        assert!(true == tree_3.search(&p));
        assert!(true == tree_3.search(&q)); 
    }

    #[test]
    pub fn test_insert_tree3() {
        let l: Vec<Field> =  vec![IntField(30), IntField(40)];
        let m: Vec<Field> =  vec![IntField(5), IntField(25)];
        let n: Vec<Field> =  vec![IntField(70), IntField(70)];
        let o: Vec<Field> =  vec![IntField(10), IntField(12)];
        let p: Vec<Field> =  vec![IntField(50), IntField(30)];
        let q: Vec<Field> =  vec![IntField(35), IntField(45)];
        let tree_3 = tree3();
        assert!(tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(l.clone()), Some(m.clone()), Some(n.clone()),
            Some(o.clone()), None, Some(p.clone()), None, None, None, None,
            None, Some(q.clone()),],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });                   
    }

    #[test]
    pub fn test_bulk_load_tree3 () {
        let l: Vec<Field> =  vec![IntField(30), IntField(40)];
        let m: Vec<Field> =  vec![IntField(5), IntField(25)];
        let n: Vec<Field> =  vec![IntField(70), IntField(70)];
        let o: Vec<Field> =  vec![IntField(10), IntField(12)];
        let p: Vec<Field> =  vec![IntField(50), IntField(30)];
        let q: Vec<Field> =  vec![IntField(35), IntField(45)];
        let mut bulk_load_tree_3 = KdTree::new(2, vec![0, 1], 2);
        let mut tree_arr_3: [Vec<Field>; 6] = [l.clone(), m.clone(), n.clone(), o.clone(), p.clone(), q.clone(),];
        bulk_load_tree_3.data_into_tree(&mut tree_arr_3);
        assert!(bulk_load_tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(q.clone()), Some(m.clone()), Some(n.clone()), 
            Some(o.clone()), Some(l.clone()),Some(p.clone()),],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });
    }

    #[test]
    pub fn test_delete_tree3() {
        let l: Vec<Field> =  vec![IntField(30), IntField(40)];
        let m: Vec<Field> =  vec![IntField(5), IntField(25)];
        let n: Vec<Field> =  vec![IntField(70), IntField(70)];
        let o: Vec<Field> =  vec![IntField(10), IntField(12)];
        let p: Vec<Field> =  vec![IntField(50), IntField(30)];
        let q: Vec<Field> =  vec![IntField(35), IntField(45)];
        let mut tree_3 = tree3();
        tree_3.delete(&l);
        tree_3.print_tree();
        assert!(tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(q.clone()), Some(m.clone()), Some(n.clone()),
            Some(o.clone()), None, Some(p.clone()), None, None, None,
            None, None, None,],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });
        tree_3 = tree3();
        tree_3.delete(&n);
        assert!(tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(l.clone()), Some(m.clone()), Some(p.clone()),
            Some(o.clone()), None, None, Some(q.clone()), None,
            None, None, None, None],
            idx_fields: vec![0, 1],
            total_dim: 2,
        });
    }

    #[test]
    pub fn test_insert_delete_tree4 () {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(8), IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(10), IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(9), IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(19), IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(20), IntField(3), IntField(6)];
        let mut bulk_load_tree_4 = KdTree::new(3, vec![0, 1, 2], 3);
        let mut tree_arr_4: [Vec<Field>; 11] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), 
        f.clone(), g.clone(), h.clone(), i.clone(), j.clone(), k.clone()];
        bulk_load_tree_4.data_into_tree(&mut tree_arr_4);
        assert!(bulk_load_tree_4 == KdTree {
            dim: 3,
            arr: vec![Some(i.clone()), Some(e.clone()), Some(f.clone()), Some(a.clone()),
            Some(c.clone()), Some(j.clone()), Some(g.clone()), Some(b.clone()), 
            None, Some(d.clone()),None, Some(k.clone()), None,
            Some(h.clone()),],
            idx_fields: vec![0, 1, 2],
            total_dim: 3,
        });
        let mut range_query_result = bulk_load_tree_4.range_query(&vec![IntField(18), IntField(3), IntField(6)], &vec![IntField(20), IntField(7), IntField(19)]);
        range_query_result.sort_by(|a, b| a[0].cmp(&b[0]));
        bulk_load_tree_4.delete(&i);
        assert!(bulk_load_tree_4 == KdTree {
            dim: 3,
            arr: vec![Some(h.clone()), Some(e.clone()), Some(f.clone()), Some(a.clone()),
            Some(c.clone()), Some(j.clone()), Some(g.clone()), Some(b.clone()), 
            None, Some(d.clone()),None, Some(k.clone()), None,
            None,],
            idx_fields: vec![0, 1, 2],
            total_dim: 3,
        });        
    }

    #[test]
    pub fn test_range_query_tree4 () {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(8), IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(10), IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(9), IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(19), IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(20), IntField(3), IntField(6)];
        let mut bulk_load_tree_4 = KdTree::new(3, vec![0, 1, 2], 3);
        let mut tree_arr_4: [Vec<Field>; 11] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), 
        f.clone(), g.clone(), h.clone(), i.clone(), j.clone(), k.clone()];
        bulk_load_tree_4.data_into_tree(&mut tree_arr_4);
        let mut range_query_result = bulk_load_tree_4.range_query(&vec![IntField(18), IntField(3), IntField(6)], &vec![IntField(20), IntField(7), IntField(19)]);
        range_query_result.sort_by(|a, b| a[0].cmp(&b[0]));
        assert!(range_query_result == vec![f.clone(), j.clone(), k.clone()]);        
    }

    pub fn test_get_tree4 () {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(8), IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(10), IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(9), IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(19), IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(20), IntField(3), IntField(6)];
        let mut bulk_load_tree_4 = KdTree::new(3, vec![0, 1, 2], 3);
        let mut tree_arr_4: [Vec<Field>; 11] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), 
        f.clone(), g.clone(), h.clone(), i.clone(), j.clone(), k.clone()];
        bulk_load_tree_4.data_into_tree(&mut tree_arr_4);
        let mut get_result = bulk_load_tree_4.get(&vec![IntField(10), IntField(21), IntField(3)]);
        get_result.sort_by(|a, b| a[0].cmp(&b[0]));
        assert!(get_result == vec![h.clone()]);        
    }

    #[test]
    pub fn test_search_tree5() {
        let a: Vec<Field> = vec![IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(3), IntField(6)];
        let mut tree_5 = tree5();
        assert!(true == tree_5.search(&a));
        assert!(true == tree_5.search(&b));
        assert!(true == tree_5.search(&c));
        assert!(true == tree_5.search(&d));
        assert!(true == tree_5.search(&e));
        assert!(true == tree_5.search(&f));
        assert!(true == tree_5.search(&g));   
        assert!(true == tree_5.search(&h));
        assert!(true == tree_5.search(&i));
        assert!(true == tree_5.search(&j));
        assert!(true == tree_5.search(&k));   
    }

    #[test]
    pub fn test_bulk_load_tree5 () {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(8), IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(10), IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(9), IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(19), IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(20), IntField(3), IntField(6)];        
        let tree_5 = tree5();
        assert!(tree_5 == KdTree {
            dim: 2,
            arr: vec![Some(e.clone()), Some(a.clone()), Some(g.clone()), Some(b.clone()),
            Some(j.clone()), Some(h.clone()), Some(c.clone()), Some(k.clone()), 
            None, None, Some(i.clone()), None, Some(d.clone()), Some(f.clone())],
            idx_fields: vec![1,2],
            total_dim: 3,
        });
    }

    #[test]
    pub fn test_delete_tree5() {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let b: Vec<Field> = vec![IntField(6), IntField(6), IntField(7)];
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let d: Vec<Field> = vec![IntField(8), IntField(21), IntField(3)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let h: Vec<Field> = vec![IntField(10), IntField(21), IntField(3)];
        let i: Vec<Field> = vec![IntField(9), IntField(5), IntField(16)];
        let j: Vec<Field> = vec![IntField(19), IntField(5), IntField(19)];  
        let k: Vec<Field> = vec![IntField(20), IntField(3), IntField(6)];
        let mut tree_5 = tree5();
        tree_5.delete(&i);
        assert!(tree_5 == KdTree {
            dim: 2,
            arr: vec![Some(e.clone()), Some(a.clone()), Some(g.clone()), Some(b.clone()),
            Some(j.clone()), Some(h.clone()), Some(c.clone()), Some(k.clone()), 
            None, None, None, None, Some(d.clone()), Some(f.clone())],
            idx_fields: vec![1, 2],
            total_dim: 3,
        });           
        tree_5.delete(&e);
        assert!(tree_5 == KdTree {
            dim: 2,
            arr: vec![Some(f.clone()), Some(a.clone()), Some(g.clone()), Some(b.clone()),
            Some(j.clone()), Some(h.clone()), Some(c.clone()), Some(k.clone()), 
            None, None, None, None, Some(d.clone()), None],
            idx_fields: vec![1, 2],
            total_dim: 3,
        });                 
    }

    #[test]
    pub fn test_get_tree5() {
        let a: Vec<Field> = vec![IntField(2), IntField(4), IntField(11)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let mut tree_5 = tree5();    
        let mut get_result = tree_5.get(&vec![IntField(4), IntField(11)]);
        get_result.sort_by(|a, b| a[0].cmp(&b[0]));
        assert!(get_result == vec![a.clone()]);      
        println!("correct here");    
        get_result = tree_5.get(&vec![IntField(7), IntField(15)]);
        get_result.sort_by(|a, b| a[0].cmp(&b[0]));
        assert!(get_result == vec![e.clone(), f.clone()]);      
    }

    #[test]
    pub fn test_range_query_tree5 () {
        let c: Vec<Field> = vec![IntField(3), IntField(15), IntField(9)];
        let e: Vec<Field> = vec![IntField(8), IntField(7), IntField(15)];
        let f: Vec<Field> = vec![IntField(18), IntField(7), IntField(15)];
        let g: Vec<Field> = vec![IntField(12), IntField(13), IntField(9)];     
        let mut tree_5 = tree5();    
        let mut range_query_result = tree_5.range_query(&vec![IntField(7), IntField(3)], &vec![IntField(16), IntField(15)]);
        range_query_result.sort_by(|a, b| a[0].cmp(&b[0]));
        assert!(range_query_result == vec![c.clone(), e.clone(), g.clone(), f.clone()]);        
    }
}
