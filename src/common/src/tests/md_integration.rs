use common::md_index::kd_tree::*;
use common::Tuple;
use common::Field;
use common::Field::{IntField, StringField};

#[test]
fn md_insert_multiple_int() {
    let a = vec![4,7];
    let b = vec![3,8];
    let c = vec![5,2];
    let d = vec![5,6];
    let e = vec![2,9];
    let f = vec![10,1];
    let g = vec![11,3];
    let a_type: Vec<Type> = vec![IntField(4), IntField(7)];
    let b_type: Vec<Type> = vec![IntField(3), IntField(8)];
    let c_type: Vec<Type> = vec![IntField(5), IntField(2)];
    let d_type: Vec<Type> = vec![IntField(5), IntField(6)];
    let e_type: Vec<Type> = vec![IntField(2), IntField(9)];
    let f_type: Vec<Type> = vec![IntField(10), IntField(1)];
    let g_type: Vec<Type> = vec![IntField(11), IntField(3)];
    let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
    let mut bulk_load_tree = KdTree::new(2, vec![0, 1]);
    let mut tree_vec_helper = Vec::new();
    let mut tree_vec_converted = KdTree::int_arr_to_type(&tree_vec, &mut tree_vec_helper);
    bulk_load_tree.data_into_tree(&mut tree_vec_converted);
    assert!(bulk_load_tree == KdTree {
        dim: 2,
        arr: vec![Some(c_type.clone()), Some(b_type.clone()), Some(g_type.clone()), Some(a_type.clone()),
        Some(e_type.clone()), Some(f_type.clone()), Some(d_type.clone())],
        idx_fields: vec![0, 1],
    });
}

#[test]
fn md_insert_single_int() {
    let a = vec![4,7];
    let b = vec![3,8];
    let c = vec![5,2];
    let d = vec![5,6];
    let e = vec![2,9];
    let f = vec![10,1];
    let g = vec![11,3];
    let a_type: Vec<Type> = vec![IntField(4), IntField(7)];
    let b_type: Vec<Type> = vec![IntField(3), IntField(8)];
    let c_type: Vec<Type> = vec![IntField(5), IntField(2)];
    let d_type: Vec<Type> = vec![IntField(5), IntField(6)];
    let e_type: Vec<Type> = vec![IntField(2), IntField(9)];
    let f_type: Vec<Type> = vec![IntField(10), IntField(1)];
    let g_type: Vec<Type> = vec![IntField(11), IntField(3)];
    let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
    let mut tree = KdTree::new(2, vec![0, 1]);
    for node in &tree_vec {
        tree.insert(&KdTree::int_val_to_type(node));
    }
    assert!(tree == KdTree {
        dim: 2,
        arr: vec![Some(a_type.clone()), Some(b_type.clone()), Some(c_type.clone()), None,
        Some(e_type.clone()), Some(f_type.clone()), Some(d_type.clone()), None, None, None,
        None, None, None, None, Some(g_type.clone())],
        idx_fields: vec![0, 1],
    });  
}

#[test]
fn md_search_int() {
    let a = vec![4,7];
    let b = vec![3,8];
    let c = vec![5,2];
    let d = vec![5,6];
    let e = vec![2,9];
    let f = vec![10,1];
    let g = vec![11,3];
    let a_type: Vec<Type> = vec![IntField(4), IntField(7)];
    let b_type: Vec<Type> = vec![IntField(3), IntField(8)];
    let c_type: Vec<Type> = vec![IntField(5), IntField(2)];
    let d_type: Vec<Type> = vec![IntField(5), IntField(6)];
    let e_type: Vec<Type> = vec![IntField(2), IntField(9)];
    let f_type: Vec<Type> = vec![IntField(10), IntField(1)];
    let g_type: Vec<Type> = vec![IntField(11), IntField(3)];
    let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
    let mut tree = KdTree::new(2, vec![0, 1]);
    for node in &tree_vec {
        tree.insert(&KdTree::int_val_to_type(node));
    }
    assert!(true == tree.search(&a_type));
    assert!(true == tree.search(&b_type));
    assert!(true == tree.search(&c_type));
    assert!(true == tree.search(&d_type));
    assert!(true == tree.search(&e_type));
    assert!(true == tree.search(&f_type));
    assert!(true == tree.search(&g_type));   
}

#[test]
fn md_delete_int() {
    let a = vec![4,7];
    let b = vec![3,8];
    let c = vec![5,2];
    let d = vec![5,6];
    let e = vec![2,9];
    let f = vec![10,1];
    let g = vec![11,3];
    let a_type: Vec<Type> = vec![IntField(4), IntField(7)];
    let b_type: Vec<Type> = vec![IntField(3), IntField(8)];
    let c_type: Vec<Type> = vec![IntField(5), IntField(2)];
    let d_type: Vec<Type> = vec![IntField(5), IntField(6)];
    let e_type: Vec<Type> = vec![IntField(2), IntField(9)];
    let f_type: Vec<Type> = vec![IntField(10), IntField(1)];
    let g_type: Vec<Type> = vec![IntField(11), IntField(3)];
    let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
    let mut tree = KdTree::new(2, vec![0, 1]);
    for node in &tree_vec {
        tree.insert(&KdTree::int_val_to_type(node));
    }
    tree.delete(&g_type);
    assert!(tree == KdTree {
        dim: 2,
        arr: vec![Some(a_type.clone()), Some(b_type.clone()), Some(c_type.clone()), None,
        Some(e_type.clone()), Some(f_type.clone()), Some(d_type.clone()), None, None, None,
        None, None, None, None, None,],
        idx_fields: vec![0, 1],
    });           
    tree.delete(&a_type);
    assert!(tree == KdTree {
        dim: 2,
        arr: vec![Some(c_type.clone()), Some(b_type.clone()), Some(d_type.clone()), None,
        Some(e_type.clone()), Some(f_type.clone()), None, None, None, None,
        None, None, None, None, None],
        idx_fields: vec![0, 1],
    });      
}

// #[test]
// fn md_insert_multiple_float() {
//     let a = vec![4.0,7.0];
//     let b = vec![3.0,8.0];
//     let c = vec![5.0,2.0];
//     let d = vec![5.0,6.0];
//     let e = vec![2.0,9.0];
//     let f = vec![10.0,1.0];
//     let g = vec![11.0,3.0]; 
//     let a_type: Vec<Type> = vec![Type::Float(4.0), Type::Float(7.0)];
//     let b_type: Vec<Type> = vec![Type::Float(3.0), Type::Float(8.0)];
//     let c_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(2.0)];
//     let d_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(6.0)];
//     let e_type: Vec<Type> = vec![Type::Float(2.0), Type::Float(9.0)];
//     let f_type: Vec<Type> = vec![Type::Float(10.0), Type::Float(1.0)];
//     let g_type: Vec<Type> = vec![Type::Float(11.0), Type::Float(3.0)];
//     let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
//     let mut bulk_load_tree = KdTree::new(2);
//     let mut tree_vec_helper = Vec::new();
//     let mut tree_vec_converted = KdTree::float_arr_to_type(&tree_vec, &mut tree_vec_helper);
//     bulk_load_tree.data_into_tree(&mut tree_vec_converted);
//     bulk_load_tree.print_tree();
//     assert!(bulk_load_tree == KdTree {
//         dim: 2,
//         arr: vec![Some(c_type.clone()), Some(b_type.clone()), Some(g_type.clone()), Some(a_type.clone()),
//         Some(e_type.clone()), Some(f_type.clone()), Some(d_type.clone())]
//     });
// }

// #[test]
// fn md_insert_single_float() {
//     let a = vec![4.0,7.0];
//     let b = vec![3.0,8.0];
//     let c = vec![5.0,2.0];
//     let d = vec![5.0,6.0];
//     let e = vec![2.0,9.0];
//     let f = vec![10.0,1.0];
//     let g = vec![11.0,3.0]; 
//     let a_type: Vec<Type> = vec![Type::Float(4.0), Type::Float(7.0)];
//     let b_type: Vec<Type> = vec![Type::Float(3.0), Type::Float(8.0)];
//     let c_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(2.0)];
//     let d_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(6.0)];
//     let e_type: Vec<Type> = vec![Type::Float(2.0), Type::Float(9.0)];
//     let f_type: Vec<Type> = vec![Type::Float(10.0), Type::Float(1.0)];
//     let g_type: Vec<Type> = vec![Type::Float(11.0), Type::Float(3.0)];
//     let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
//     let mut tree = KdTree::new(2);
//     for node in &tree_vec {
//         tree.insert(&KdTree::float_val_to_type(node));
//     }
//     assert!(tree == KdTree {
//         dim: 2,
//         arr: vec![Some(a_type.clone()), Some(b_type.clone()), Some(c_type.clone()), None,
//         Some(e_type.clone()), Some(f_type.clone()), Some(d_type.clone()), None, None, None,
//         None, None, None, None, Some(g_type.clone())],
//     });  
// }

// #[test]
// fn md_search_float() {
//     let a = vec![4.0,7.0];
//     let b = vec![3.0,8.0];
//     let c = vec![5.0,2.0];
//     let d = vec![5.0,6.0];
//     let e = vec![2.0,9.0];
//     let f = vec![10.0,1.0];
//     let g = vec![11.0,3.0]; 
//     let a_type: Vec<Type> = vec![Type::Float(4.0), Type::Float(7.0)];
//     let b_type: Vec<Type> = vec![Type::Float(3.0), Type::Float(8.0)];
//     let c_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(2.0)];
//     let d_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(6.0)];
//     let e_type: Vec<Type> = vec![Type::Float(2.0), Type::Float(9.0)];
//     let f_type: Vec<Type> = vec![Type::Float(10.0), Type::Float(1.0)];
//     let g_type: Vec<Type> = vec![Type::Float(11.0), Type::Float(3.0)];
//     let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
//     let mut tree = KdTree::new(2);
//     for node in &tree_vec {
//         tree.insert(&KdTree::float_val_to_type(node));
//     }
//     assert!(true == tree.search(&a_type));
//     assert!(true == tree.search(&b_type));
//     assert!(true == tree.search(&c_type));
//     assert!(true == tree.search(&d_type));
//     assert!(true == tree.search(&e_type));
//     assert!(true == tree.search(&f_type));
//     assert!(true == tree.search(&g_type));   
// }

// #[test]
// fn md_delete_float() {
//     let a = vec![4.0,7.0];
//     let b = vec![3.0,8.0];
//     let c = vec![5.0,2.0];
//     let d = vec![5.0,6.0];
//     let e = vec![2.0,9.0];
//     let f = vec![10.0,1.0];
//     let g = vec![11.0,3.0]; 
//     let a_type: Vec<Type> = vec![Type::Float(4.0), Type::Float(7.0)];
//     let b_type: Vec<Type> = vec![Type::Float(3.0), Type::Float(8.0)];
//     let c_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(2.0)];
//     let d_type: Vec<Type> = vec![Type::Float(5.0), Type::Float(6.0)];
//     let e_type: Vec<Type> = vec![Type::Float(2.0), Type::Float(9.0)];
//     let f_type: Vec<Type> = vec![Type::Float(10.0), Type::Float(1.0)];
//     let g_type: Vec<Type> = vec![Type::Float(11.0), Type::Float(3.0)];
//     let tree_vec = vec![a.clone(), b.clone(), c.clone(), d.clone(), e.clone(), f.clone(), g.clone()];
//     let mut tree = KdTree::new(2);
//     for node in &tree_vec {
//         tree.insert(&KdTree::float_val_to_type(node));
//     }
//     tree.delete(&g_type);
//     assert!(tree == KdTree {
//         dim: 2,
//         arr: vec![Some(a_type.clone()), Some(b_type.clone()), Some(c_type.clone()), None,
//         Some(e_type.clone()), Some(f_type.clone()), Some(d_type.clone()), None, None, None,
//         None, None, None, None, None,],
//     });           
//     tree.delete(&a_type);
//     assert!(tree == KdTree {
//         dim: 2,
//         arr: vec![Some(c_type.clone()), Some(b_type.clone()), Some(d_type.clone()), None,
//         Some(e_type.clone()), Some(f_type.clone()), None, None, None, None,
//         None, None, None, None, None],
//     });      
// }

