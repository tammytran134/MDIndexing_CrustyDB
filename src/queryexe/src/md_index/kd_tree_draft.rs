use std::cmp::Ordering::{Less, Greater, Equal};
use std::ops::{Deref};
use std::sync::{Arc, RwLock};

// #[derive(Clone, PartialEq, PartialOrd)]
// pub enum Type {
//     Num(i64),
//     Float(f64),
//     Str(String),
// }

// use Type::{Num, Float, Str};
// #[derive(PartialEq)]
// pub struct Node<'a> {
//     val: Option<Arc<RwLock<&'a Vec<Type>>>>,
//     left: Option<Arc<RwLock<Box<&'a Node<'a>>>>>,
//     right: Option<Arc<RwLock<Box<&'a Node<'a>>>>>,
// }

// pub struct KdTree<'a> {
//     dim: usize,
//     head: &'a mut Node<'a>,
//     //arr: &'a mut Vec<Vec<Type>>,
// }

// impl<'a> Node<'a>  {
//     fn new(val: Option<Arc<RwLock<&'a Vec<Type>>>>, left: Option<Arc<RwLock<Box<&'a Node<'a>>>>>, right: Option<Arc<RwLock<Box<&'a Node<'a>>>>>) -> Self {
//         Self {
//             val,
//             left,
//             right,
//         }
//     }

//     fn compare_val(val1: &'a Vec<Type>, val2: &'a Vec<Type>) -> bool {
//         if val1.len() == 0 {
//             if val2.len() == 0 {
//                 return true
//             }
//             else {
//                 return false
//             }
//         }
//         else {
//             for i in 0..val1.len() {
//                 match (&val1[i], &val2[i]) {    
//                     (Num(x), Num(y)) => {
//                         if x != y {
//                             return false
//                         }
//                     },         
//                     (Float(x), Float(y)) => {
//                         if x != y {
//                             return false
//                         }
//                     },                
//                     (Str(x), Str(y)) => {
//                         if !x.eq(y) {
//                             return false
//                         }
//                     }, 
//                     (_, _) => {
//                         return false
//                     }
//                 }  
//             }
//             return true
//         }
//     }

//     fn compare_val_at_dim(val1: &Vec<Type>, val2: &Vec<Type>, dim: usize) -> i8 {
//         let curr_idx = dim;
//         match (&val1[curr_idx], &val2[curr_idx]) {    
//             (Num(x), Num(y)) => {
//                 if x <= y {
//                     return -1
//                 }
//                 else if x == y {
//                     return 0
//                 }
//                 else {
//                     return 1
//                 }
//             },         
//             (Float(x), Float(y)) => {
//                 if x <= y {
//                     return -1
//                 }
//                 else if x == y {
//                     return 0
//                 }
//                 else {
//                     return 1
//                 }
//             },                
//             (Str(x), Str(y)) => {
//                 match x.cmp(&y) {
//                     Less => -1,
//                     Equal => 0,
//                     Greater => 1,
//                 }
//             }, 
//             (_, _) => {
//                 return 0
//             }
//         }             
//     }

//     fn insert_helper(&mut self, val: &'a Vec<Type>, depth: usize, dim: usize) {
//         if self.val.is_none() {
//             self.val = Some(Arc::new(RwLock::new(val)));
//             return
//         }
//         let curr_dim = depth % dim;
//         // let next_node = if Node::compare_val_at_dim(val, &self.val.unwrap(), curr_dim) < 0 {&mut self.left} else {&mut self.right};
//         // match next_node {
//         //     &mut Some(ref mut sub_node) => {
//         //         sub_node.insert_helper(val, depth + 1, dim); return},
//         //     &mut None => {
//         //         let mut new_node = Node::new(Some(val), None, None);
//         //         let boxed_node = Some(Box::new(new_node));
//         //         *next_node = boxed_node;
//         //         return
//         //     }
//         // }
//         let mut next_node = if Node::compare_val_at_dim(val, &*self.val.as_ref().unwrap().write().unwrap(), curr_dim) < 0 {self.left} else {self.right};
//         match next_node.is_none() {
//             true => {
//                 let mut new_node = Node::new(Some(Arc::new(RwLock::new(val))), None, None);
//                 next_node = Some(Arc::new(RwLock::new(Box::new(&new_node))));
//                 return
//             }
//             false => match next_node.as_ref().unwrap().write().unwrap() {
//                 ref sub_node => {sub_node.insert_helper(val, depth + 1, dim); return},}
//         }
//     }

//     fn insert(&mut self, val: &'a Vec<Type>, dim: usize) {
//         self.insert_helper(val, 0, dim)
//     }

//     fn data_into_tree_helper(&mut self, arr: &'a Vec<Vec<Type>>, start: usize, end: usize, depth: usize, dim: usize) {
//         let len_arr = end - start;
//         if len_arr <= 1 {
//             return
//         }
//         let curr_dim = depth % dim;
//         //arr[start..end].to_vec().sort_by(|a, b| a[curr_dim].cmp(&b[curr_dim]));
//         //find median
//         let median = start + len_arr/2;
//         self.insert(&arr[median], 0);
//         self.data_into_tree_helper(&arr, start, median, depth + 1, dim);
//         self.data_into_tree_helper(&arr, median + 1, end, depth + 1, dim);
//     }

//     fn data_into_tree(&mut self, arr: &'a mut Vec<Vec<Type>>, dim: usize) {
//         let len_arr = arr.len();
//         if len_arr == 0 {
//             //error
//         }
//         self.data_into_tree_helper(arr, 0, len_arr, 0, dim)

//     }

//     fn search_helper(&self, val: &'a Vec<Type>, depth: usize, dim: usize) -> bool {
//         if self.val.is_none() {
//             return false;
//         }
//         let curr_dim = depth % dim;
//         if Node::compare_val(&self.val.as_ref().unwrap().read().unwrap(), val) {
//             true
//         }
//         else {
//             if Node::compare_val_at_dim(val, &self.val.as_ref().unwrap().read().unwrap(), curr_dim) < 0 && self.left.is_some() {
//                 self.left.as_ref().unwrap().read().unwrap().search_helper(val, depth + 1, dim)
//             }
//             else if Node::compare_val_at_dim(val, &self.val.as_ref().unwrap().read().unwrap(), curr_dim) >= 0 && self.right.is_some() {
//                 self.right.as_ref().unwrap().read().unwrap().search_helper(val, depth + 1, dim)
//             }
//             else {
//                 false
//             }
//         }
//     }

//     fn search(&self, val: &'a Vec<Type>, dim: usize) -> bool {
//         self.search_helper(val, 0, dim)
//     }

//     fn print_val(&self) {
//         if self.val.is_none() {
//             println!("Empty tree");
//         }
//         println!("[");
//         let value_arr = self.val.as_ref().unwrap().read().unwrap();
//         for element in value_arr {
//             match element {
//                 Num(x) => {println!("{},", x)},
//                 Float(x) => {println!("{},", x)},
//                 Str(x) => {println!("{},", x)},                
//             }
//         }
//         println!("]\n");
//     }

//     // fn cmp_min_node(head: Option<Arc<&'a Node<'a>>>, val_left: Option<Arc<&'a Node<'a>>>, val_right: Option<Arc<&'a Node<'a>>>, curr_dim: usize) -> Option<Arc<&'a Node<'a>>> {
//     //     let res = head;
//     //     if val_left.is_some() && Node::compare_val_at_dim(&*val_left.unwrap().val.as_ref().unwrap(), *res.unwrap().val.unwrap(), curr_dim) < 0 {
//     //         res = val_left;
//     //     }
//     //     if val_right.is_some() && Node::compare_val_at_dim(&*val_right.unwrap().val.as_ref().unwrap(), *res.unwrap().val.unwrap(), curr_dim) < 0 {
//     //         res = val_right;
//     //     }
//     //     return res;
//     // }

//     // fn find_min_helper(&self, curr_dim: usize, dim: usize, depth: usize) -> Option<Arc<&'a Node<'a>>> {
//     //     if self.val.is_none() {
//     //         return None
//     //     }        
//     //     let curr_dim2 = depth % dim;
//     //     if curr_dim2 == curr_dim {
//     //         if self.left.is_none() {
//     //             return Some(Arc::new(self));
//     //         }
//     //         else {
//     //             return self.left.as_ref().unwrap().find_min_helper(curr_dim, dim, depth + 1);
//     //         }
//     //     }
//     //     return Node::cmp_min_node(Some(Arc::new(self)), self.left.as_ref().unwrap().find_min_helper(curr_dim, dim, depth + 1),
//     //     self.right.as_ref().unwrap().find_min_helper(curr_dim, dim, depth + 1), curr_dim);
//     // }

//     // fn find_min(&self, curr_dim: usize, dim: usize) -> Option<Arc<&'a Node<'a>>> {
//     //     self.find_min_helper(curr_dim, dim, 0)
//     // }

//     // fn copy_new_val(&mut self, min_node: Option<Arc<&'a Node<'a>>>, dim: usize) {
//     //     if min_node.is_none() {
//     //         self.val = None;
//     //     }
//     //     let old_val = self.val.unwrap();
//     //     let new_val = min_node.unwrap().val.unwrap();
//     //     for i in 0..dim {
//     //         old_val[i] = new_val[i];
//     //     }
//     // }

//     // fn delete_helper(&mut self, val: &Vec<Type>, depth: usize, dim: usize) -> Option<&Node>{
//     //     if self.val.is_none() {
//     //         return None
//     //     }
//     //     let curr_dim = depth % dim;
//     //     if Node::compare_val(&self.val.unwrap(), val) {
//     //         if self.right.is_some() {
//     //             let min_node = self.right.as_ref().unwrap().find_min(curr_dim, dim);
//     //             self.copy_new_val(min_node, dim);
//     //             self.right = Some(Arc::new(Box::new(self.right.as_ref().unwrap().delete_helper(*min_node.unwrap().val.unwrap(), depth + 1, dim).unwrap())));        
//     //         }
//     //         else if self.left.is_some() {
//     //             let min_node = self.left.as_ref().unwrap().find_min(curr_dim, dim);
//     //             self.copy_new_val(min_node, dim);
//     //             self.right = Some(Arc::new(Box::new(self.left.as_ref().unwrap().delete_helper(*min_node.unwrap().val.unwrap(), depth + 1, dim).unwrap())));                      
//     //         }
//     //         else {
//     //             self.val = None;
//     //             return None;
//     //         }
//     //         return Some(self)
//     //     }
//     //     else {
//     //         if Node::compare_val_at_dim(val, *self.val.unwrap(), curr_dim) < 0 {
//     //             self.left.as_ref().unwrap().delete_helper(val, depth + 1, dim)
//     //         }
//     //         else {
//     //             self.right.as_ref().unwrap().delete_helper(val, depth + 1, dim)
//     //         }
//     //     }
//     // }

//     // fn delete(&mut self, val: &Vec<Type>, dim: usize) -> Option<&Node>{
//     //     self.delete_helper(val, 0, dim)
//     // }

//     fn print_node_helper(&self, depth: usize) {
//         println!("depth level: {}\n", depth);
//         self.print_val();
//         if self.left.is_some() {
//             println!("left\n");
//             self.left.as_ref().unwrap().read().unwrap().print_node_helper(depth + 1);
//         }
//         if self.right.is_some() {
//             println!("right\n");
//             self.right.as_ref().unwrap().read().unwrap().print_node_helper(depth + 1);
//         }
//     }

//     fn print_node(&self) {
//         self.print_node_helper(0)
//     }
// }

// impl<'a> KdTree<'a> {
//     fn new(dim: usize, head: &'a mut Node<'a>) -> Self {
//         Self {
//             dim,
//             head,
//         }
//     }
// }

// fn main() {
//     let mut a = vec![Type::Num(4), Type::Num(7)];
//     let mut b = vec![Type::Num(3), Type::Num(8)];
//     let mut c = vec![Type::Num(5), Type::Num(2)];
//     let mut d = vec![Type::Num(5), Type::Num(6)];
//     let mut e = vec![Type::Num(2), Type::Num(9)];
//     let mut f = vec![Type::Num(10), Type::Num(1)];
//     let mut g = vec![Type::Num(11), Type::Num(3)];
//     let mut node = Node::new(None, None, None);
//     node.insert(&a, 2);
//     node.insert(&b, 2);
//     node.insert(&c, 2);
//     node.insert(&d, 2);
//     node.insert(&e, 2);
//     node.insert(&f, 2);
//     node.insert(&g, 2);
//     assert!(true == node.search(&a, 2));
//     assert!(true == node.search(&b, 2));
//     assert!(true == node.search(&c, 2));
//     assert!(true == node.search(&d, 2));
//     assert!(true == node.search(&e, 2));
//     assert!(true == node.search(&f, 2));
//     assert!(true == node.search(&g, 2));
//     assert!(node == Node {
//         val: Some(Arc::new(&a)),
//         left: Some(Arc::new(Box::new(&Node {
//             val: Some(Arc::new(&b)),
//             left: None,
//             right: Some(Arc::new(Box::new(&Node { val: Some(Arc::new(&e)), left: None, right: None }))),
//         }))),
//         right: Some(Arc::new(Box::new(&Node {
//             val: Some(Arc::new(&c)),
//             left: Some(Arc::new(Box::new(&Node { val: Some(Arc::new(&f)), left: None, right: None }))),
//             right: Some(Arc::new(Box::new(&Node { 
//                 val: Some(Arc::new(&d)), 
//                 left: None, 
//                 right: Some(Arc::new(Box::new(&Node { val: Some(Arc::new(&g)), left: None, right: None })))}))),
//         }))),
//     });
// }

