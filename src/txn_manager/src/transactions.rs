use common::ids::TransactionId;
use common::CrustyError;

/// Transaction implementation.
pub struct Transaction {
    tid: TransactionId,
    started: bool,
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new() -> Self {
        Self {
            tid: TransactionId::new(),
            started: false,
        }
    }

    /// Starts the transaction.
    pub fn start(&mut self) {
        self.started = true
    }

    /// Returns the transaction id.
    pub fn tid(&self) -> TransactionId {
        self.tid
    }

    /// Commits the transaction.
    pub fn commit(&mut self) -> Result<(), CrustyError> {
        self.complete(true)
    }

    /// Aborts the transaction.
    pub fn abort(&mut self) -> Result<(), CrustyError> {
        self.complete(false)
    }

    /// Completes the transaction.
    ///
    /// # Arguments
    ///
    /// * `commit` - True if the transaction should commit.
    pub fn complete(&mut self, commit: bool) -> Result<(), CrustyError> {
        if self.started {
            error!("Error: FIXME, need to notify on txn complete {}", commit);
            //FIXME DBSERVER.transaction_complete(self.tid, commit)?;
            self.started = false;
        }
        Ok(())
    }
}

