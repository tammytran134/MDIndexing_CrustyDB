use super::OpIterator;
use common::{CrustyError, TableSchema, Tuple};

/// Projection operator.
pub struct ProjectIterator {
    fields: Vec<usize>,
    open: bool,
    schema: TableSchema,
    child: Box<dyn OpIterator>,
}

impl ProjectIterator {
    /// Constructor for the projection operator without aliases.
    ///
    /// # Arguments
    ///
    /// * `fields` - Columns to project.
    /// * `child` - Child nodes to get data from.
    pub fn new(fields: Vec<usize>, child: Box<dyn OpIterator>) -> Self {
        let mut attributes = Vec::new();
        for i in &fields {
            let attr = child.get_schema().get_attribute(*i).unwrap();
            attributes.push(attr.clone());
        }
        let schema = TableSchema::new(attributes);
        Self {
            fields,
            open: false,
            schema,
            child,
        }
    }

    /// Constructor for the projection operator with aliases.
    ///
    /// # Arguments
    ///
    /// * `fields` - List of field indices to project.
    /// * `field_names` - Aliases of the fields in the final projection.
    /// * `child` - Child nodes to get data from.
    ///
    /// # Notes
    ///
    /// `field_names` has to correspond to `fields`.
    pub fn new_with_aliases(
        fields: Vec<usize>,
        field_names: Vec<&str>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        let mut attributes = Vec::new();
        let child_schema = child.get_schema();
        for (i, name) in fields.iter().zip(field_names.iter()) {
            let mut attr = child_schema.get_attribute(*i).unwrap().clone();
            attr.name = name.to_string();
            attributes.push(attr);
        }
        let schema = TableSchema::new(attributes);
        Self {
            fields,
            open: false,
            schema,
            child,
        }
    }
}

impl OpIterator for ProjectIterator {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.child.open()?;
        self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        let next = self.child.next()?;
        if let Some(tuple) = next {
            let mut new_field_vals = Vec::new();
            for i in &self.fields {
                let t = match tuple.get_field(*i) {
                    None => panic!("No such field"),
                    Some(t) => t,
                };
                new_field_vals.push(t.clone());
            }
            return Ok(Some(Tuple::new(new_field_vals)));
        }
        Ok(next)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.child.rewind()?;
        self.close()?;
        self.open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::opiterator::testutil::*;

    use common::testutil::*;
    const WIDTH: usize = 3;

    fn get_project(fields: Vec<usize>) -> ProjectIterator {
        let tuples = create_tuple_list(vec![vec![0, 1, 2], vec![0, 1, 2], vec![0, 1, 2]]);
        let schema = get_int_table_schema(WIDTH);
        let ti = TupleIterator::new(tuples.to_vec(), schema);
        ProjectIterator::new(fields, Box::new(ti))
    }

    #[test]
    fn test_open() -> Result<(), CrustyError> {
        let mut project = get_project(vec![0]);
        assert!(!project.open);
        project.open()?;
        assert!(project.open);
        Ok(())
    }

    #[test]
    fn test_next() -> Result<(), CrustyError> {
        let mut project = get_project(vec![1, 2]);
        project.open()?;
        assert_eq!(sum_int_fields(&mut project)?, 9);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_next_not_open() {
        let mut project = get_project(vec![0]);
        project.next().unwrap();
    }

    #[test]
    fn test_close() -> Result<(), CrustyError> {
        let mut project = get_project(vec![0, 1, 2]);
        project.open()?;
        assert!(project.open);
        project.close()?;
        assert!(!project.open);
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_rewind_not_open() {
        let mut project = get_project(vec![0]);
        project.rewind().unwrap();
    }

    #[test]
    fn test_rewind() -> Result<(), CrustyError> {
        let mut project = get_project(vec![1, 2]);
        project.open()?;
        let sum_before = sum_int_fields(&mut project);
        project.rewind()?;
        let sum_after = sum_int_fields(&mut project);
        assert_eq!(sum_before, sum_after);
        Ok(())
    }
}
