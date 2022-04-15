use std::cmp::Ordering::{Less, Greater, Equal};
use std::ops::{Deref};

#[derive(Clone, PartialEq, PartialOrd)]
pub enum Type {
    Num(i64),
    Float(f64),
    Str(String),
}

// impl Iterator for Type {
//     type Item = Type;
//     fn next(&mut self) -> Option<Self::Item> {

//     }
// }

use Type::{Num, Float, Str};
#[derive(PartialEq)]
pub struct Node<'a> {
    val: &'a Vec<Type>,
    left: Option<Box<Node<'a>>>,
    right: Option<Box<Node<'a>>>,
}

pub struct KdTree<'a> {
    dim: usize,
    head: &'a mut Node<'a>,
    //arr: &'a mut Vec<Vec<Type>>,
}

impl<'a> Node<'a>  {
    fn new(val: &'a Vec<Type>, left: Option<Box<Node<'a>>>, right: Option<Box<Node<'a>>>) -> Self {
        Self {
            val: val,
            left,
            right,
        }
    }

    fn compare_val(val1: &'a Vec<Type>, val2: &'a Vec<Type>) -> bool {
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
        if val1.len() < (dim + 1).into() {
            //error
            return 0
        }
        else {
            let curr_idx = usize::from(dim);
            match (&val1[curr_idx], &val2[curr_idx]) {    
                (Num(x), Num(y)) => {
                    if x <= y {
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
                    if x <= y {
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
    }

    fn insert_helper(&mut self, val: &'a Vec<Type>, depth: usize, dim: usize) {
        let curr_dim = depth & dim;
        let next_node = if Node::compare_val_at_dim(&self.val, val, curr_dim) <= 0 {&mut self.left} else {&mut self.right};
        match next_node {
            &mut Some(ref mut sub_node) => {
                sub_node.insert_helper(val, depth + 1, dim); return},
            &mut None => {
                let mut new_node = Node::new(val, None, None);
                let boxed_node = Some(Box::new(new_node));
                *next_node = boxed_node;
                return
            }
        }
    }

    fn insert(&mut self, val: &'a Vec<Type>, dim: usize) {
        self.insert_helper(val, 0, dim)
    }

    fn data_into_tree_helper(&mut self, arr: &'a Vec<Vec<Type>>, start: usize, end: usize, depth: usize, dim: usize) {
        let len_arr = end - start;
        if len_arr <= 1 {
            return
        }
        let curr_dim = depth % dim;
        //arr[start..end].to_vec().sort_by(|a, b| a[curr_dim].cmp(&b[curr_dim]));
        //find median
        let median = start + usize::from(len_arr/2);
        self.insert(&arr[median], 0);
        self.data_into_tree_helper(&arr, start, median, depth + 1, dim);
        self.data_into_tree_helper(&arr, median + 1, end, depth + 1, dim);
    }

    fn data_into_tree(&mut self, arr: &'a mut Vec<Vec<Type>>, dim: usize) {
        let len_arr = arr.len();
        if len_arr == 0 {
            //error
        }
        self.data_into_tree_helper(arr, 0, len_arr, 0, dim)

    }

    fn search_helper(&self, val: &'a Vec<Type>, depth: usize, dim: usize) -> bool {
        let curr_dim = depth % dim;
        if Node::compare_val(&self.val, val) {
            true
        }
        else {
            if Node::compare_val_at_dim(&self.val, val, curr_dim) <= 0 && self.left.is_some() {
                self.left.as_ref().unwrap().search_helper(val, depth + 1, dim)
            }
            else if Node::compare_val_at_dim(&self.val, val, curr_dim) > 0 && self.left.is_some() {
                self.right.as_ref().unwrap().search_helper(val, depth + 1, dim)
            }
            else {
                false
            }
        }
    }

    fn search(&self, val: &'a Vec<Type>, dim: usize) -> bool {
        self.search_helper(val, 0, dim)
    }


    fn delete(&self, val: &Vec<Type>) {

    }
}

impl<'a> KdTree<'a> {
    fn new(dim: usize, head: &'a mut Node<'a>) -> Self {
        Self {
            dim,
            head,
        }
    }
}

fn main() {
    let mut a = vec![Type::Num(4), Type::Num(7)];
    let mut b = vec![Type::Num(3), Type::Num(8)];
    let mut c = vec![Type::Num(5), Type::Num(2)];
    let mut d = vec![Type::Num(5), Type::Num(6)];
    let mut e = vec![Type::Num(2), Type::Num(9)];
    let mut f = vec![Type::Num(10), Type::Num(1)];
    let mut g = vec![Type::Num(11), Type::Num(3)];
    let mut node = Node::new(&a, None, None);
    node.insert(&b, 2);
    node.insert(&c, 2);
    node.insert(&d, 2);
    node.insert(&e, 2);
    node.insert(&f, 2);
    node.insert(&g, 2);
    assert!(true, node.search(&a, 2));
    assert!(true, node.search(&b, 2));
    assert!(true, node.search(&c, 2));
    assert!(true, node.search(&d, 2));
    assert!(true, node.search(&e, 2));
    assert!(true, node.search(&f, 2));
    assert!(true, node.search(&g, 2));
    assert!(node == Node {
        val: &a,
        left: Some(Box::new(Node {
            val: &b,
            left: Some(Box::new(Node { val: &d, left: None, right: None })),
            right: Some(Box::new(Node { val: &e, left: None, right: None })),
        })),
        right: Some(Box::new(Node {
            val: &c,
            left: Some(Box::new(Node { val: &f, left: None, right: None })),
            right: Some(Box::new(Node { val: &g, left: None, right: None })),
        })),
    });
}

