use std::cmp::Ordering::{Less, Greater, Equal};
use std::any::type_name;
use crate::Tuple;
use crate::Field;
use std::cmp::Ordering;
use crate::Field::{IntField, StringField};
use rstar::{RTree, RTreeObject, AABB, PointDistance};

#[derive(Clone, PartialEq, PartialOrd)]
pub struct Tuple2d {
    pub idx_fields: Vec<usize>,
    vals: Vec<Field>,
}

impl RTreeObject for Tuple2d {
    type Envelope = AABB<[i32; 2]>;
    fn envelope(&self) -> Self::Envelope
    {
        AABB::from_point([self.vals[self.idx_fields[0]].unwrap_int_field(), self.vals[self.idx_fields[1]].unwrap_int_field()])
    }    
}

#[derive(Clone)]
pub struct RTree2d {
    pub r_tree: RTree<Tuple2d>,
    pub idx_fields: Vec<usize>,
    total_dim: usize,
}

impl PointDistance for Tuple2d
{
    fn distance_2(&self, point: &[i32; 2]) -> i32
    {
        let d_x = self.vals[self.idx_fields[0]].unwrap_int_field() - point[0];
        let d_y = self.vals[self.idx_fields[1]].unwrap_int_field() - point[1];
        d_x.pow(2) + d_y.pow(2)
    }
}

#[derive(Clone, PartialEq, PartialOrd)]
pub struct Tuple3d {
    pub idx_fields: Vec<usize>,
    vals: Vec<Field>,
}

impl RTreeObject for Tuple3d {
    type Envelope = AABB<[i32; 3]>;
    fn envelope(&self) -> Self::Envelope
    {
        AABB::from_point([self.vals[self.idx_fields[0]].unwrap_int_field(), self.vals[self.idx_fields[1]].unwrap_int_field(), self.vals[self.idx_fields[2]].unwrap_int_field()])
    }    
}

#[derive(Clone)]
pub struct RTree3d {
    pub r_tree: RTree<Tuple3d>,
    pub idx_fields: Vec<usize>,
    total_dim: usize,
}

impl PointDistance for Tuple3d
{
    fn distance_2(&self, point: &[i32; 3]) -> i32
    {
        let d_x = self.vals[self.idx_fields[0]].unwrap_int_field() - point[0];
        let d_y = self.vals[self.idx_fields[1]].unwrap_int_field() - point[1];
        let d_z = self.vals[self.idx_fields[2]].unwrap_int_field() - point[2];
        d_x.pow(2) + d_y.pow(2)+ d_z.pow(2)
    }
}

pub enum R_Tree {
    Dim2(RTree2d),
    Dim3(RTree3d),
}

impl R_Tree {

    fn copy_single_val(val: &Field) -> Field {
        match val {
            IntField(x) => {IntField(*x)},
            StringField(x) => {StringField(x.to_string())},
        }        
    }

    fn get_new_copy(val: &Vec<Field>, total_dim: usize) -> Vec<Field> {
        let mut new_copy = Vec::new();
        for i in 0..val.len() {
            new_copy.push(R_Tree::copy_single_val(&val[i]));
        }
        return new_copy
    }

    pub fn new(dim: usize, idx_fields: Vec<usize>, total_dim: usize) -> Self {
        if dim == 2 {
            R_Tree::Dim2(RTree2d {r_tree: RTree::new(), idx_fields, total_dim})
        }
        else if dim == 3 {
            R_Tree::Dim3(RTree3d {r_tree: RTree::new(), idx_fields, total_dim})
        }
        else {
            panic!("Dimension not supported")
        }
    }
    
    pub fn insert(&mut self, val: &Vec<Field>) {
        match self {
            R_Tree::Dim2(tree) => tree.r_tree.insert(Tuple2d {idx_fields: tree.idx_fields.clone(), vals: R_Tree::get_new_copy(val, tree.total_dim)}),
            R_Tree::Dim3(tree) => tree.r_tree.insert(Tuple3d {idx_fields: tree.idx_fields.clone(), vals: R_Tree::get_new_copy(val, tree.total_dim)}),
        }
    }

    pub fn search(&mut self, val: &Vec<Field>) -> bool {
        match self {
            R_Tree::Dim2(tree) => tree.r_tree.contains(&Tuple2d {idx_fields: tree.idx_fields.clone(), vals: R_Tree::get_new_copy(val, tree.total_dim)}),
            R_Tree::Dim3(tree) => tree.r_tree.contains(&Tuple3d {idx_fields: tree.idx_fields.clone(), vals: R_Tree::get_new_copy(val, tree.total_dim)}), 
        }       
    }

    pub fn val_to_i32_2d(val:&Vec<Field>) -> [i32;2] {
        let mut arr: [i32; 2] = [0;2];
        let mut i = 0;
        for single_field in val {
            arr[i] = single_field.unwrap_int_field();
            i += 1;
        }
        return arr;
    }

    pub fn val_to_i32_3d(val:&Vec<Field>) -> [i32;3] {
        let mut arr: [i32; 3] = [0;3];
        let mut i = 0;
        for single_field in val {
            arr[i] = single_field.unwrap_int_field();
            i += 1;
        }
        return arr;
    }

    pub fn get(&mut self, val: &Vec<Field>) -> Vec<Vec<Field>> {
        match self {
            R_Tree::Dim2(tree) => {
                let mut res_iter = tree.r_tree.locate_all_at_point(&R_Tree::val_to_i32_2d(val));
                let mut res = Vec::new();
                while let Some(tuple) = res_iter.next() {
                    res.push(tuple.vals.clone());
                }
                return res;
                },
            R_Tree::Dim3(tree) => {
                let mut res_iter = tree.r_tree.locate_all_at_point(&R_Tree::val_to_i32_3d(val));
                let mut res = Vec::new();
                while let Some(tuple) = res_iter.next() {
                    res.push(tuple.vals.clone());
                }
                return res;
                }, 
        }       
    }

    pub fn knn(&mut self, val:&Vec<Field>, k: usize) -> Vec<Vec<Field>> {
        
        match self {
            R_Tree::Dim2(tree) => {
                                    let mut heap_iter = tree.r_tree.nearest_neighbor_iter(&R_Tree::val_to_i32_2d(val));
                                    let mut res = Vec::new();
                                    for i in 0..k {
                                        let neighbor = heap_iter.next();
                                        if neighbor.is_none() {
                                            return res;
                                        }
                                        else {
                                            res.push(neighbor.unwrap().vals.clone());
                                        }
                                    }
                                    return res;
                                    },
            R_Tree::Dim3(tree) => {let mut heap_iter = tree.r_tree.nearest_neighbor_iter(&R_Tree::val_to_i32_3d(val));
                let mut res = Vec::new();
                for i in 0..k {
                    let neighbor = heap_iter.next();
                    if neighbor.is_none() {
                        return res;
                    }
                    else {
                        res.push(neighbor.unwrap().vals.clone());
                    }
                }
                return res;
            },
        }    
    }

    pub fn data_into_tree(&mut self, arr: &mut [Vec<Field>]) {
        let len_arr = arr.len();
        if len_arr == 0 {
            return
        }
        let mut vec_arr_2d = Vec::new();
        let mut vec_arr_3d = Vec::new();
        for tuple in arr {
            match self {
                R_Tree::Dim2(tree) => vec_arr_2d.push(Tuple2d {idx_fields: tree.idx_fields.clone(), vals: tuple.clone()}),
                R_Tree::Dim3(tree) => vec_arr_3d.push(Tuple3d {idx_fields: tree.idx_fields.clone(), vals: tuple.clone()}),
            }  
        }
        match self {
            R_Tree::Dim2(tree) => tree.r_tree = RTree::bulk_load(vec_arr_2d.clone()),
            R_Tree::Dim3(tree) => tree.r_tree = RTree::bulk_load(vec_arr_3d.clone())
        }   
    }

    pub fn delete(&mut self, val: &Vec<Field>) {
        match self {
            R_Tree::Dim2(tree) => {tree.r_tree.remove(&Tuple2d {idx_fields: tree.idx_fields.clone(), vals: R_Tree::get_new_copy(val, tree.total_dim)});},
            R_Tree::Dim3(tree) => {tree.r_tree.remove(&Tuple3d {idx_fields: tree.idx_fields.clone(), vals: R_Tree::get_new_copy(val, tree.total_dim)});},
        }
        return
    }

    pub fn get_idx_fields(&self) -> Vec<usize> {
        match self {
            R_Tree::Dim2(tree) => tree.idx_fields.clone(),
            R_Tree::Dim3(tree) => tree.idx_fields.clone(),
        }        
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testutil::*;
    use std::{println as debug};

    pub fn tree5() -> R_Tree {
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
        let mut tree5 = R_Tree::new(2, vec![1, 2], 3);
        let mut tree_arr_5: [Vec<Field>; 11] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), 
        f.clone(), g.clone(), h.clone(), i.clone(), j.clone(), k.clone()];
        tree5.data_into_tree(&mut tree_arr_5);
        tree5
    }

    pub fn tree6() -> R_Tree {
        let a: Vec<Field> = vec![IntField(5), IntField(4)];
        let b: Vec<Field> = vec![IntField(2), IntField(6)];
        let c: Vec<Field> = vec![IntField(13), IntField(3)];
        let d: Vec<Field> = vec![IntField(3), IntField(1)];
        let e: Vec<Field> = vec![IntField(10), IntField(2)];
        let f: Vec<Field> = vec![IntField(8), IntField(7)];
        let mut tree6 = R_Tree::new(2, vec![0, 1], 2);
        tree6.insert(&a);
        tree6.insert(&b);
        tree6.insert(&c);
        tree6.insert(&d);
        tree6.insert(&e);
        tree6.insert(&f);
        tree6
    }

    pub fn tree7() -> R_Tree {
        let a: Vec<Field> = vec![IntField(5), IntField(23)];
        let b: Vec<Field> = vec![IntField(6), IntField(28)];
        let c: Vec<Field> = vec![IntField(8), IntField(24)];
        let d: Vec<Field> = vec![IntField(10), IntField(28)];
        let e: Vec<Field> = vec![IntField(9), IntField(26)];
        let f: Vec<Field> = vec![IntField(10), IntField(30)];
        let g: Vec<Field> = vec![IntField(7), IntField(22)];
        let h: Vec<Field> = vec![IntField(9), IntField(18)];
        let i: Vec<Field> = vec![IntField(6), IntField(24)];
        let j: Vec<Field> = vec![IntField(8), IntField(32)];  
        let k: Vec<Field> = vec![IntField(9), IntField(20)];
        let l: Vec<Field> =  vec![IntField(7), IntField(32)];
        let m: Vec<Field> =  vec![IntField(7), IntField(27)];
        let n: Vec<Field> =  vec![IntField(18), IntField(30)];
        let o: Vec<Field> =  vec![IntField(11), IntField(22)];
        let p: Vec<Field> =  vec![IntField(17), IntField(31)];
        let q: Vec<Field> =  vec![IntField(13), IntField(28)];
        let r: Vec<Field> =  vec![IntField(16), IntField(24)];
        let s: Vec<Field> =  vec![IntField(19), IntField(26)];
        let t: Vec<Field> =  vec![IntField(10), IntField(28)];
        let u: Vec<Field> =  vec![IntField(14), IntField(27)];
        let v: Vec<Field> =  vec![IntField(18), IntField(23)];
        let w: Vec<Field> =  vec![IntField(17), IntField(22)];
        let x: Vec<Field> =  vec![IntField(18), IntField(24)];
        let mut tree7 = R_Tree::new(2, vec![0, 1], 2);
        let mut tree_arr_7: [Vec<Field>; 24] = [a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), 
        f.clone(), g.clone(), h.clone(), i.clone(), j.clone(), k.clone(), l.clone(), m.clone(), n.clone(),
        o.clone(), p.clone(), q.clone(), r.clone(), s.clone(), t.clone(), u.clone(), v.clone(), w.clone(),
        x.clone()];
        tree7.data_into_tree(&mut tree_arr_7);
        tree7
    }


#[test]

pub fn test_r_tree_library() {
    let mut tree = RTree::new();
    tree.insert([0.1, 0.0f32, 0.1]);
    tree.insert([0.2, 0.1, 0.1]);
    tree.insert([0.3, 0.0, 0.1]);
    
    assert_eq!(tree.nearest_neighbor(&[0.4, -0.1, 0.1]), Some(&[0.3, 0.0, 0.1]));
    tree.remove(&[0.3, 0.0, 0.1]);
    assert_eq!(tree.nearest_neighbor(&[0.4, 0.3, 0.1]), Some(&[0.2, 0.1, 0.1]));
    
    assert_eq!(tree.size(), 2);
    // &RTree implements IntoIterator!
    for point in &tree {
        println!("Tree contains a point {:?}", point);
    }    
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
    get_result = tree_5.get(&vec![IntField(7), IntField(15)]);
    get_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(get_result == vec![e.clone(), f.clone()]);      
}

#[test]
pub fn test_knn_tree6 () {
    let a: Vec<Field> = vec![IntField(5), IntField(4)];
    let b: Vec<Field> = vec![IntField(2), IntField(6)];
    let c: Vec<Field> = vec![IntField(13), IntField(3)];
    let d: Vec<Field> = vec![IntField(3), IntField(1)];
    let e: Vec<Field> = vec![IntField(10), IntField(2)];
    let f: Vec<Field> = vec![IntField(8), IntField(7)];
    let mut tree_6 = tree6();    
    let mut knn_result = tree_6.knn(&vec![IntField(9), IntField(4)], 1);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![e.clone()]); 
    knn_result = tree_6.knn(&vec![IntField(9), IntField(4)], 2);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![f.clone(), e.clone()]); 
    knn_result = tree_6.knn(&vec![IntField(9), IntField(4)], 3);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![a.clone(), f.clone(), e.clone()]); 
    knn_result = tree_6.knn(&vec![IntField(9), IntField(4)], 4);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![a.clone(), f.clone(), e.clone(), c.clone()]);     
    knn_result = tree_6.knn(&vec![IntField(9), IntField(4)], 5);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![d.clone(), a.clone(), f.clone(), e.clone(), c.clone()]); 
    knn_result = tree_6.knn(&vec![IntField(5), IntField(6)], 1);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![a.clone()]);    
    knn_result = tree_6.knn(&vec![IntField(5), IntField(6)], 2);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![b.clone(), a.clone()]);  
    knn_result = tree_6.knn(&vec![IntField(5), IntField(6)], 3);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![b.clone(), a.clone(), f.clone()]);                 
}

#[test]
pub fn test_knn_tree7 () {
    let a: Vec<Field> = vec![IntField(5), IntField(23)];
    let b: Vec<Field> = vec![IntField(6), IntField(28)];
    let c: Vec<Field> = vec![IntField(8), IntField(24)];
    let d: Vec<Field> = vec![IntField(10), IntField(28)];
    let e: Vec<Field> = vec![IntField(9), IntField(26)];
    let f: Vec<Field> = vec![IntField(10), IntField(30)];
    let g: Vec<Field> = vec![IntField(7), IntField(22)];
    let h: Vec<Field> = vec![IntField(9), IntField(18)];
    let i: Vec<Field> = vec![IntField(6), IntField(24)];
    let j: Vec<Field> = vec![IntField(8), IntField(32)];  
    let k: Vec<Field> = vec![IntField(9), IntField(20)];
    let l: Vec<Field> =  vec![IntField(7), IntField(32)];
    let m: Vec<Field> =  vec![IntField(7), IntField(27)];
    let n: Vec<Field> =  vec![IntField(18), IntField(30)];
    let o: Vec<Field> =  vec![IntField(11), IntField(22)];
    let p: Vec<Field> =  vec![IntField(17), IntField(31)];
    let q: Vec<Field> =  vec![IntField(13), IntField(28)];
    let r: Vec<Field> =  vec![IntField(16), IntField(24)];
    let s: Vec<Field> =  vec![IntField(19), IntField(26)];
    let t: Vec<Field> =  vec![IntField(10), IntField(28)];
    let u: Vec<Field> =  vec![IntField(14), IntField(27)];
    let v: Vec<Field> =  vec![IntField(18), IntField(23)];
    let w: Vec<Field> =  vec![IntField(17), IntField(22)];
    let x: Vec<Field> =  vec![IntField(18), IntField(24)];    
    let mut tree_7 = tree7();    
    let mut knn_result = tree_7.knn(&vec![IntField(10), IntField(28)], 3);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0])); 
    assert!(knn_result == vec![d.clone(), t.clone(),f.clone(),]);     
    knn_result = tree_7.knn(&vec![IntField(7), IntField(18)], 3);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));     
    assert!(knn_result == vec![g.clone(), h.clone(), k.clone()]);      
    knn_result = tree_7.knn(&vec![IntField(3), IntField(25)], 3);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![a.clone(), i.clone(), b.clone()]);      
    knn_result = tree_7.knn(&vec![IntField(15), IntField(29)], 2);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![q.clone(), u.clone()]);    
    knn_result = tree_7.knn(&vec![IntField(1), IntField(2)], 2);
    knn_result.sort_by(|a, b| a[0].cmp(&b[0]));
    assert!(knn_result == vec![h.clone(), k.clone()]);               
}
}