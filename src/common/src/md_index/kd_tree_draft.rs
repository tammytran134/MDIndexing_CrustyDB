use std::cmp::Ordering::{Less, Greater, Equal};
use std::any::type_name;
#[derive(Clone, PartialEq, PartialOrd)]
pub enum Type {
    Num(i64),
    Float(f64),
    Str(String),
}

use Type::{Num, Float, Str};
#[derive(Clone, PartialEq, PartialOrd)]
pub struct KdTree {
    pub dim: usize,
    pub arr: Vec<Option<Vec<Type>>>,
}


impl KdTree {
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            arr: Vec::new(),
        }
    }

    fn compare_val(val1: &Vec<Type>, val2: &Vec<Type>) -> bool {
        if val1.len() == 0 {
            if val2.len() == 0 {
                return true
            }
            else {
                return false
            }
        }
        else {
            for i in 0..val1.len() {
                match (&val1[i], &val2[i]) {    
                    (Num(x), Num(y)) => {
                        if x != y {
                            return false
                        }
                    },         
                    (Float(x), Float(y)) => {
                        if x != y {
                            return false
                        }
                    },                
                    (Str(x), Str(y)) => {
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

    fn compare_val_at_dim(val1: &Vec<Type>, val2: &Vec<Type>, dim: usize) -> i8 {
        let curr_idx = dim;
        match (&val1[curr_idx], &val2[curr_idx]) {    
            (Num(x), Num(y)) => {
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
            (Float(x), Float(y)) => {
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
            (Str(x), Str(y)) => {
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

    fn get_val(&self, val: &Vec<Type>) -> Vec<Type> {
        let mut res = Vec::new();
        for i in 0..self.dim {
            match &val[i] {
                Num(x) => {res.push(Num(*x));}
                Float(x) => {res.push(Float(*x));}
                Str(x) => {res.push(Str(x.to_string()));}
            }
        }
        return res;
    }

    fn print_val2(&self, val: &Vec<Type>) {
        if val.len() == 0 {
            println!("Empty val");
        }
        println!("[");
        for element in val {
            match element {
                Num(x) => {println!("{},", x)},
                Float(x) => {println!("{},", x)},
                Str(x) => {println!("{},", x)},                
            }
        }
        println!("]\n");
    }

    fn insert_helper(&mut self, val: &Vec<Type>, node_idx: usize, depth: usize) {
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
        if KdTree::compare_val_at_dim(&val, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
            self.insert_helper(val, node_idx*2+1, depth + 1)
        }
        else {
            self.insert_helper(val, node_idx*2+2, depth + 1)
        }
    }

    pub fn insert(&mut self, val: &Vec<Type>) {
        self.insert_helper(val, 0, 0)
    } 

    fn search_helper(&mut self, val: &Vec<Type>, node_idx: usize, depth: usize) -> bool {
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx {
            return false
        }
        if self.arr[node_idx].is_none() {
            return false
        }
        if KdTree::compare_val(&self.arr[node_idx].as_ref().unwrap(), val) {
            return true
        }
        let curr_dim = depth % self.dim;
        if KdTree::compare_val_at_dim(val, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
            self.search_helper(val, node_idx*2+1, depth + 1)
        }
        else {
            self.search_helper(val, node_idx*2+2, depth + 1)
        }
    }

    pub fn search(&mut self, val: &Vec<Type>) -> bool {
        self.search_helper(val, 0, 0)
    }

    fn copy_from_vec(&self, array: &mut [Vec<Type>], vec: &Vec<Vec<Type>>) {
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


    fn print_arr(&self, array: &[Vec<Type>]) {
        let arr_len = array.len();
        if arr_len == 0 {
            println!("Empty arr\n");
        }  
        println!("[");
        for element in array {
            println!("[");
            for single_val in element {
                match single_val {
                    Num(x) => {println!("{},", x)},
                    Float(x) => {println!("{},", x)},
                    Str(x) => {println!("{},", x)},                
                }
            }
            println!("], ");
        }  
        println!("]\n");
    }

    fn print_vec(&self, vec: &Vec<Vec<Type>>) {
        let vec_len = vec.len();
        if vec_len == 0 {
            println!("Empty vec\n");
        }  
        println!("[");
        for element in vec {
            println!("[");
            for single_val in element {
                match single_val {
                    Num(x) => {println!("{},", x)},
                    Float(x) => {println!("{},", x)},
                    Str(x) => {println!("{},", x)},                
                }
            }
            println!("], ");
        }  
        println!("]\n");
    }
    
    fn sort(&self, array: &mut [Vec<Type>], curr_dim: usize) {
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
      
      fn merge(&self,l_arr: &[Vec<Type>], r_arr: &[Vec<Type>], sorted: &mut [Vec<Type>], curr_dim: usize) {
        // Current loop position in left half, right half, and sorted vector
        let (mut left, mut right, mut i) = (0, 0, 0);
        while left < l_arr.len() && right < r_arr.len() {
          if KdTree::compare_val_at_dim(&l_arr[left], &r_arr[right], curr_dim) <= 0 {
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

    fn data_into_tree_helper(&mut self, arr: &mut [Vec<Type>], depth: usize) {
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

    pub fn data_into_tree(&mut self, arr: &mut [Vec<Type>]) {
        let len_arr = arr.len();
        if len_arr == 0 {
            return
        }
        self.data_into_tree_helper(arr, 0)

    }

    fn cmp_min_node(&self, val1: usize, val2: Option<usize>, val3: Option<usize>, curr_dim: usize) -> Option<usize> {
        let mut res = val1;
        let arr_len = self.arr.len();
        if val2.is_some() && arr_len > val2.unwrap() && self.arr[val2.unwrap()].is_some() &&
        KdTree::compare_val_at_dim(&self.arr[val2.unwrap()].as_ref().unwrap(), &self.arr[val1].as_ref().unwrap(), curr_dim) < 0 {
            res = val2.unwrap();
        }
        if val3.is_some() && arr_len > val3.unwrap() && self.arr[val3.unwrap()].is_some() &&
        KdTree::compare_val_at_dim(&self.arr[val3.unwrap()].as_ref().unwrap(), &self.arr[val1].as_ref().unwrap(), curr_dim) < 0 {
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

    fn copy_single_val(&self, val: &Type) -> Type {
        match val {
            Num(x) => {Num(*x)},
            Float(x) => {Float(*x)},
            Str(x) => {Str(x.to_string())},
        }        
    }

    fn get_new_copy(&self, node_idx: usize) -> Vec<Type> {
        let mut new_copy = Vec::new();
        let copy_val = self.arr[node_idx].as_ref().unwrap();
        for i in 0..self.dim {
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

    fn delete_helper(&mut self, val: &Vec<Type>, node_idx: usize, depth: usize) {
        //println!("node idx is {}", node_idx);
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx || self.arr[node_idx].is_none() {
            return
        }
        let curr_dim = depth % self.dim;
        if KdTree::compare_val(&self.arr[node_idx].as_ref().unwrap(), val) {
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
            if KdTree::compare_val_at_dim(val, &self.arr[node_idx].as_ref().unwrap(), curr_dim) < 0 {
                //println!("turn left");
                self.delete_helper(val, node_idx * 2 + 1, depth + 1)
            }
            else {
                //println!("turn right");
                self.delete_helper(val, node_idx * 2 + 2, depth + 1)
            }
        }
    }

    pub fn delete(&mut self, val: &Vec<Type>) {
        self.delete_helper(val, 0, 0)
    }

    fn print_val(val: &Option<Vec<Type>>) {
        if val.is_none() {
            println!("Empty tree");
        }
        let val_arr = val.as_ref().unwrap();
        println!("[");
        for element in val_arr {
            match element {
                Num(x) => {println!("{},", x)},
                Float(x) => {println!("{},", x)},
                Str(x) => {println!("{},", x)},                
            }
        }
        println!("]\n");
    }

    fn print_tree_helper(&self, node_idx: usize, depth: usize) {
        println!("depth level: {}\n", depth);
        let arr_len = self.arr.len();
        if arr_len == 0 || arr_len <= node_idx {
            return 
        }
        KdTree::print_val(&self.arr[node_idx]);
        if arr_len > (node_idx*2 + 1) && self.arr[node_idx*2 + 1].is_some() {
            println!("left\n");
            self.print_tree_helper(node_idx*2 + 1, depth + 1);
        }
        if arr_len > (node_idx*2 + 2) && self.arr[node_idx*2 + 2].is_some() {
            println!("right\n");
            self.print_tree_helper(node_idx*2 + 2, depth + 1);
        }
    }

    pub fn print_tree(&self) {
        self.print_tree_helper(0, 0)
    }
    
    pub fn int_val_to_type(vec: &Vec<i64>) -> Vec<Type> {
        let mut res: Vec<Type> = Vec::new();
        for element in vec {
            res.push(Num(*element));
        }
        return res;
    }

    pub fn float_val_to_type(vec: &Vec<f64>) -> Vec<Type> {
        let mut res: Vec<Type> = Vec::new();
        for element in vec {
            res.push(Float(*element));
        }
        return res;
    }

    pub fn str_val_to_type(vec: &Vec<String>) -> Vec<Type> {
        let mut res: Vec<Type> = Vec::new();
        for element in vec {
            res.push(Str(element.to_string()));
        }
        return res;
    }

    pub fn int_arr_to_type<'a>(vec: &'a Vec<Vec<i64>>, res: &'a mut Vec<Vec<Type>>) -> &'a mut [Vec<Type>]  {
        for element in vec {
            res.push(KdTree::int_val_to_type(element));
        }
        return &mut res[..];
    }

    pub fn float_arr_to_type<'a>(vec: &'a Vec<Vec<f64>>, res: &'a mut Vec<Vec<Type>>) -> &'a mut [Vec<Type>]  {
        for element in vec {
            res.push(KdTree::float_val_to_type(element));
        }
        return &mut res[..];
    }

    pub fn str_arr_to_type<'a>(vec: &'a Vec<Vec<String>>, res: &'a mut Vec<Vec<Type>>) -> &'a mut [Vec<Type>]  {
        for element in vec {
            res.push(KdTree::str_val_to_type(element));
        }
        return &mut res[..];
    }

}

fn main() {
    
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testutil::*;

    pub fn tree1() -> KdTree {
        let a: Vec<Type> = vec![Type::Num(4), Type::Num(7)];
        let b: Vec<Type> = vec![Type::Num(3), Type::Num(8)];
        let c: Vec<Type> = vec![Type::Num(5), Type::Num(2)];
        let d: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let e: Vec<Type> = vec![Type::Num(2), Type::Num(9)];
        let f: Vec<Type> = vec![Type::Num(10), Type::Num(1)];
        let g: Vec<Type> = vec![Type::Num(11), Type::Num(3)];
        let mut tree = KdTree::new(2);
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
        let h: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let i: Vec<Type> = vec![Type::Num(4), Type::Num(10)];
        let j: Vec<Type> = vec![Type::Num(4), Type::Num(20)];
        let k: Vec<Type> =  vec![Type::Num(4), Type::Num(30)];
        let mut tree = KdTree::new(2);
        tree.insert(&h);
        tree.insert(&i);
        tree.insert(&j);
        tree.insert(&k);
        tree
    }

    pub fn tree3() -> KdTree {
        let l: Vec<Type> =  vec![Type::Num(30), Type::Num(40)];
        let m: Vec<Type> =  vec![Type::Num(5), Type::Num(25)];
        let n: Vec<Type> =  vec![Type::Num(70), Type::Num(70)];
        let o: Vec<Type> =  vec![Type::Num(10), Type::Num(12)];
        let p: Vec<Type> =  vec![Type::Num(50), Type::Num(30)];
        let q: Vec<Type> =  vec![Type::Num(35), Type::Num(45)];
        let mut tree = KdTree::new(2);
        let mut tree_arr: [&Vec<Type>; 6] = [&l, &m, &n, &o, &p, &q];
        for element in tree_arr {
            tree.insert(element);
        }
        tree 
    }

    pub fn tree4() -> KdTree {
        let a: Vec<Type> = vec![Type::Num(2), Type::Num(4), Type::Num(11)];
        let b: Vec<Type> = vec![Type::Num(6), Type::Num(6), Type::Num(7)];
        let c: Vec<Type> = vec![Type::Num(3), Type::Num(15), Type::Num(9)];
        let d: Vec<Type> = vec![Type::Num(8), Type::Num(21), Type::Num(3)];
        let e: Vec<Type> = vec![Type::Num(8), Type::Num(7), Type::Num(15)];
        let f: Vec<Type> = vec![Type::Num(18), Type::Num(7), Type::Num(15)];
        let g: Vec<Type> = vec![Type::Num(12), Type::Num(13), Type::Num(9)];     
        let h: Vec<Type> = vec![Type::Num(10), Type::Num(21), Type::Num(3)];
        let i: Vec<Type> = vec![Type::Num(9), Type::Num(5), Type::Num(16)];
        let j: Vec<Type> = vec![Type::Num(19), Type::Num(5), Type::Num(19)];  
        let k: Vec<Type> = vec![Type::Num(20), Type::Num(3), Type::Num(6)];
        let mut tree = KdTree::new(2);
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

    #[test]
    pub fn test_int_arr_to_type() {
        let a = vec![4,7];
        let b = vec![3,8];
        let c = vec![5,2];
        let d = vec![5,6];
        let e = vec![2,9];
        let f = vec![10,1];
        let g = vec![11,3]; 
        let a_type: Vec<Type> = vec![Type::Num(4), Type::Num(7)];
        let b_type: Vec<Type> = vec![Type::Num(3), Type::Num(8)];
        let c_type: Vec<Type> = vec![Type::Num(5), Type::Num(2)];
        let d_type: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let e_type: Vec<Type> = vec![Type::Num(2), Type::Num(9)];
        let f_type: Vec<Type> = vec![Type::Num(10), Type::Num(1)];
        let g_type: Vec<Type> = vec![Type::Num(11), Type::Num(3)];
        let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
        let mut res = Vec::new();
        let tree_vec_converted = KdTree::int_arr_to_type(&tree_vec, &mut res);
        assert!(tree_vec_converted == &[a_type.clone(), b_type.clone(), c_type.clone(), 
        d_type.clone(), e_type.clone(), f_type.clone(), g_type.clone()]);
    }

    #[test]
    pub fn test_float_arr_to_type() {
        let a = vec![4.0,7.0];
        let b = vec![3.0,8.0];
        let c = vec![5.0,2.0];
        let d = vec![5.0,6.0];
        let e = vec![2.0,9.0];
        let f = vec![10.0,1.0];
        let g = vec![11.0,3.0]; 
        let a_type: Vec<Type> = vec![Type::Float(4.0), Type::Float(7.0)];
        let b_type: Vec<Type> = vec![Type::Float(3.0), Type::Float(8.0)];
        let c_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(2.0)];
        let d_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(6.0)];
        let e_type: Vec<Type> = vec![Type::Float(2.0), Type::Float(9.0)];
        let f_type: Vec<Type> = vec![Type::Float(10.0), Type::Float(1.0)];
        let g_type: Vec<Type> = vec![Type::Float(11.0), Type::Float(3.0)];
        let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
        let mut res = Vec::new();
        let tree_vec_converted = KdTree::float_arr_to_type(&tree_vec, &mut res);
        assert!(tree_vec_converted == &[a_type.clone(), b_type.clone(), c_type.clone(), 
        d_type.clone(), e_type.clone(), f_type.clone(), g_type.clone()]);
    }

    #[test]
    pub fn test_search_tree1() {
        let a: Vec<Type> = vec![Type::Num(4), Type::Num(7)];
        let b: Vec<Type> = vec![Type::Num(3), Type::Num(8)];
        let c: Vec<Type> = vec![Type::Num(5), Type::Num(2)];
        let d: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let e: Vec<Type> = vec![Type::Num(2), Type::Num(9)];
        let f: Vec<Type> = vec![Type::Num(10), Type::Num(1)];
        let g: Vec<Type> = vec![Type::Num(11), Type::Num(3)];
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
        let a: Vec<Type> = vec![Type::Num(4), Type::Num(7)];
        let b: Vec<Type> = vec![Type::Num(3), Type::Num(8)];
        let c: Vec<Type> = vec![Type::Num(5), Type::Num(2)];
        let d: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let e: Vec<Type> = vec![Type::Num(2), Type::Num(9)];
        let f: Vec<Type> = vec![Type::Num(10), Type::Num(1)];
        let g: Vec<Type> = vec![Type::Num(11), Type::Num(3)];
        let tree_1 = tree1();
        assert!(tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(a.clone()), Some(b.clone()), Some(c.clone()), None,
            Some(e.clone()), Some(f.clone()), Some(d.clone()), None, None, None,
            None, None, None, None, Some(g.clone())],
        });     
    }

    #[test]
    pub fn test_bulk_load_tree1 () {
        let a: Vec<Type> = vec![Type::Num(4), Type::Num(7)];
        let b: Vec<Type> = vec![Type::Num(3), Type::Num(8)];
        let c: Vec<Type> = vec![Type::Num(5), Type::Num(2)];
        let d: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let e: Vec<Type> = vec![Type::Num(2), Type::Num(9)];
        let f: Vec<Type> = vec![Type::Num(10), Type::Num(1)];
        let g: Vec<Type> = vec![Type::Num(11), Type::Num(3)];
        let mut bulk_load_tree_1 = KdTree::new(2);
        let mut tree_arr_1: [Vec<Type>; 7] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
        bulk_load_tree_1.data_into_tree(&mut tree_arr_1);
        assert!(bulk_load_tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(c.clone()), Some(b.clone()), Some(g.clone()), Some(a.clone()),
            Some(e.clone()), Some(f.clone()), Some(d.clone())]
        });
    }

    #[test]
    pub fn test_delete_tree1() {
        let a: Vec<Type> = vec![Type::Num(4), Type::Num(7)];
        let b: Vec<Type> = vec![Type::Num(3), Type::Num(8)];
        let c: Vec<Type> = vec![Type::Num(5), Type::Num(2)];
        let d: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let e: Vec<Type> = vec![Type::Num(2), Type::Num(9)];
        let f: Vec<Type> = vec![Type::Num(10), Type::Num(1)];
        let g: Vec<Type> = vec![Type::Num(11), Type::Num(3)];
        let mut tree_1 = tree1();
        tree_1.delete(&g);
        assert!(tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(a.clone()), Some(b.clone()), Some(c.clone()), None,
            Some(e.clone()), Some(f.clone()), Some(d.clone()), None, None, None,
            None, None, None, None, None,],
        });           
        tree_1.delete(&a);
        assert!(tree_1 == KdTree {
            dim: 2,
            arr: vec![Some(c.clone()), Some(b.clone()), Some(d.clone()), None,
            Some(e.clone()), Some(f.clone()), None, None, None, None,
            None, None, None, None, None],
        });                 
    }

    #[test]
    pub fn test_search_tree2() {
        let h: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let i: Vec<Type> = vec![Type::Num(4), Type::Num(10)];
        let j: Vec<Type> = vec![Type::Num(4), Type::Num(20)];
        let k: Vec<Type> =  vec![Type::Num(4), Type::Num(30)];
        let mut tree_2 = tree2();
        assert!(true == tree_2.search(&h));
        assert!(true == tree_2.search(&i));
        assert!(true == tree_2.search(&j));
        assert!(true == tree_2.search(&k));       
    }

    #[test]
    pub fn test_insert_tree2() {
        let h: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let i: Vec<Type> = vec![Type::Num(4), Type::Num(10)];
        let j: Vec<Type> = vec![Type::Num(4), Type::Num(20)];
        let k: Vec<Type> =  vec![Type::Num(4), Type::Num(30)];
        let tree_2 = tree2();
        assert!(tree_2 == KdTree {
            dim: 2,
            arr: vec![Some(h.clone()), Some(i.clone()), None, None,
            Some(j.clone()), None, None, None, None, None,
            Some(k.clone()),],
        });                   
    }

    #[test]
    pub fn test_bulk_load_tree2 () {
        let h: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let i: Vec<Type> = vec![Type::Num(4), Type::Num(10)];
        let j: Vec<Type> = vec![Type::Num(4), Type::Num(20)];
        let k: Vec<Type> =  vec![Type::Num(4), Type::Num(30)];
        let mut bulk_load_tree_2 = KdTree::new(2);
        let mut tree_arr_2: [Vec<Type>; 4] = [h.clone(), i.clone(), j.clone(), k.clone(),];
        bulk_load_tree_2.data_into_tree(&mut tree_arr_2);
        assert!(bulk_load_tree_2 == KdTree {
            dim: 2,
            arr: vec![Some(k.clone()), None, Some(j.clone()), None,
            None, Some(i.clone()), None, None, None, None, None,
            None, Some(h.clone()), ]
        });
    }

    #[test]
    pub fn test_delete_tree2() {
        let h: Vec<Type> = vec![Type::Num(5), Type::Num(6)];
        let i: Vec<Type> = vec![Type::Num(4), Type::Num(10)];
        let j: Vec<Type> = vec![Type::Num(4), Type::Num(20)];
        let k: Vec<Type> =  vec![Type::Num(4), Type::Num(30)];
        let mut tree_2 = tree2();
        tree_2.delete(&h);
        assert!(tree_2 == KdTree {
            dim: 2,
            arr: vec![Some(i.clone()), None, Some(j.clone()), None,
            None, None, Some(k.clone()), None, None, None, None,]
        });
    }

    #[test]
    pub fn test_search_tree3() {
        let l: Vec<Type> =  vec![Type::Num(30), Type::Num(40)];
        let m: Vec<Type> =  vec![Type::Num(5), Type::Num(25)];
        let n: Vec<Type> =  vec![Type::Num(70), Type::Num(70)];
        let o: Vec<Type> =  vec![Type::Num(10), Type::Num(12)];
        let p: Vec<Type> =  vec![Type::Num(50), Type::Num(30)];
        let q: Vec<Type> =  vec![Type::Num(35), Type::Num(45)];
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
        let l: Vec<Type> =  vec![Type::Num(30), Type::Num(40)];
        let m: Vec<Type> =  vec![Type::Num(5), Type::Num(25)];
        let n: Vec<Type> =  vec![Type::Num(70), Type::Num(70)];
        let o: Vec<Type> =  vec![Type::Num(10), Type::Num(12)];
        let p: Vec<Type> =  vec![Type::Num(50), Type::Num(30)];
        let q: Vec<Type> =  vec![Type::Num(35), Type::Num(45)];
        let tree_3 = tree3();
        assert!(tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(l.clone()), Some(m.clone()), Some(n.clone()),
            Some(o.clone()), None, Some(p.clone()), None, None, None, None,
            None, Some(q.clone()),],
        });                   
    }

    #[test]
    pub fn test_bulk_load_tree3 () {
        let l: Vec<Type> =  vec![Type::Num(30), Type::Num(40)];
        let m: Vec<Type> =  vec![Type::Num(5), Type::Num(25)];
        let n: Vec<Type> =  vec![Type::Num(70), Type::Num(70)];
        let o: Vec<Type> =  vec![Type::Num(10), Type::Num(12)];
        let p: Vec<Type> =  vec![Type::Num(50), Type::Num(30)];
        let q: Vec<Type> =  vec![Type::Num(35), Type::Num(45)];
        let mut bulk_load_tree_3 = KdTree::new(2);
        let mut tree_arr_3: [Vec<Type>; 6] = [l.clone(), m.clone(), n.clone(), o.clone(), p.clone(), q.clone(),];
        bulk_load_tree_3.data_into_tree(&mut tree_arr_3);
        assert!(bulk_load_tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(q.clone()), Some(m.clone()), Some(n.clone()), 
            Some(o.clone()), Some(l.clone()),Some(p.clone()),]
        });
    }

    #[test]
    pub fn test_delete_tree3() {
        let l: Vec<Type> =  vec![Type::Num(30), Type::Num(40)];
        let m: Vec<Type> =  vec![Type::Num(5), Type::Num(25)];
        let n: Vec<Type> =  vec![Type::Num(70), Type::Num(70)];
        let o: Vec<Type> =  vec![Type::Num(10), Type::Num(12)];
        let p: Vec<Type> =  vec![Type::Num(50), Type::Num(30)];
        let q: Vec<Type> =  vec![Type::Num(35), Type::Num(45)];
        let mut tree_3 = tree3();
        tree_3.delete(&l);
        tree_3.print_tree();
        assert!(tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(q.clone()), Some(m.clone()), Some(n.clone()),
            Some(o.clone()), None, Some(p.clone()), None, None, None,
            None, None, None,]
        });
        tree_3 = tree3();
        tree_3.delete(&n);
        assert!(tree_3 == KdTree {
            dim: 2,
            arr: vec![Some(l.clone()), Some(m.clone()), Some(p.clone()),
            Some(o.clone()), None, None, Some(q.clone()), None,
            None, None, None, None]
        });
    }

    #[test]
    pub fn test_tree4 () {
        let a: Vec<Type> = vec![Type::Num(2), Type::Num(4), Type::Num(11)];
        let b: Vec<Type> = vec![Type::Num(6), Type::Num(6), Type::Num(7)];
        let c: Vec<Type> = vec![Type::Num(3), Type::Num(15), Type::Num(9)];
        let d: Vec<Type> = vec![Type::Num(8), Type::Num(21), Type::Num(3)];
        let e: Vec<Type> = vec![Type::Num(8), Type::Num(7), Type::Num(15)];
        let f: Vec<Type> = vec![Type::Num(18), Type::Num(7), Type::Num(15)];
        let g: Vec<Type> = vec![Type::Num(12), Type::Num(13), Type::Num(9)];     
        let h: Vec<Type> = vec![Type::Num(10), Type::Num(21), Type::Num(3)];
        let i: Vec<Type> = vec![Type::Num(9), Type::Num(5), Type::Num(16)];
        let j: Vec<Type> = vec![Type::Num(19), Type::Num(5), Type::Num(19)];  
        let k: Vec<Type> = vec![Type::Num(20), Type::Num(3), Type::Num(6)];
        let mut bulk_load_tree_4 = KdTree::new(3);
        let mut tree_arr_4: [Vec<Type>; 11] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), 
        f.clone(), g.clone(), h.clone(), i.clone(), j.clone(), k.clone()];
        bulk_load_tree_4.data_into_tree(&mut tree_arr_4);
        assert!(bulk_load_tree_4 == KdTree {
            dim: 3,
            arr: vec![Some(i.clone()), Some(e.clone()), Some(f.clone()), Some(a.clone()),
            Some(c.clone()), Some(j.clone()), Some(g.clone()), Some(b.clone()), 
            None, Some(d.clone()),None, Some(k.clone()), None,
            Some(h.clone()),]
        });
        bulk_load_tree_4.delete(&i);
        bulk_load_tree_4.print_tree();
        assert!(bulk_load_tree_4 == KdTree {
            dim: 3,
            arr: vec![Some(h.clone()), Some(e.clone()), Some(f.clone()), Some(a.clone()),
            Some(c.clone()), Some(j.clone()), Some(g.clone()), Some(b.clone()), 
            None, Some(d.clone()),None, Some(k.clone()), None,
            None,]
        });        
    }
}
