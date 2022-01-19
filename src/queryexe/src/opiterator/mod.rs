pub use self::aggregate::Aggregate;
pub use self::filter::{Filter, FilterPredicate};
pub use self::join::{HashEqJoin, Join, JoinPredicate};
pub use self::project::ProjectIterator;
pub use self::seqscan::SeqScan;
pub use self::tuple_iterator::TupleIterator;
use common::{CrustyError, TableSchema, Tuple};

mod aggregate;
mod filter;
mod join;
mod project;
mod seqscan;
mod testutil;
mod tuple_iterator;

pub trait OpIterator {
    /// Opens the iterator. This must be called before any of the other methods.
    fn open(&mut self) -> Result<(), CrustyError>;

    /// Advances the iterator and returns the next tuple from the operator.
    ///
    /// Returns None when iteration is finished.
    ///
    /// # Panics
    ///
    /// Panic if iterator is not open.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError>;

    /// Closes the iterator.
    fn close(&mut self) -> Result<(), CrustyError>;

    /// Returns the iterator to the start.
    ///
    /// Returns None when iteration is finished.
    ///
    /// # Panics
    ///
    /// Panic if iterator is not open.
    fn rewind(&mut self) -> Result<(), CrustyError>;

    /// Returns the schema associated with this OpIterator.
    fn get_schema(&self) -> &TableSchema;
}
