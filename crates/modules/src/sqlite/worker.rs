use super::*;
use fs2::FileExt;
use std::fs::{File, OpenOptions};

pub(super) struct Worker {
    path: PathBuf,
    pub options: SqliteOptions,
    connection: Option<Connection>,
    lock: Option<File>,
    owner: Option<String>,
}
impl Worker {
    pub fn new(path: PathBuf, options: SqliteOptions) -> Self {
        Self {
            path,
            options,
            connection: None,
            lock: None,
            owner: None,
        }
    }
    pub fn claim(&mut self, owner: &str) -> Result<(), ForgeError> {
        if self.owner.is_some() || owner.is_empty() {
            return Err(conflict());
        }
        let path = if self.path.exists() {
            std::fs::canonicalize(&self.path).map_err(|_| failed())?
        } else {
            let parent = self.path.parent().ok_or_else(failed)?;
            std::fs::canonicalize(parent)
                .map_err(|_| failed())?
                .join(self.path.file_name().ok_or_else(failed)?)
        };
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".owner.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|_| failed())?;
        FileExt::try_lock_exclusive(&lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                conflict()
            } else {
                failed()
            }
        })?;
        let mut connection = Connection::open(&path).map_err(db_error)?;
        schema::initialize(&mut connection, &self.options)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        if tx
            .execute("UPDATE wf_meta SET owner=?1 WHERE id=1", [owner])
            .map_err(db_error)?
            != 1
        {
            return Err(corrupt());
        }
        // A staging reference cannot have been returned to a caller. This is
        // safe only after the new runtime holds exclusive ownership.
        tx.execute("DELETE FROM wf_artifacts WHERE state='staging'", [])
            .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        self.connection = Some(connection);
        self.lock = Some(lock);
        self.owner = Some(owner.into());
        Ok(())
    }
    pub fn release(&mut self, owner: &str) -> Result<(), ForgeError> {
        if self.owner.as_deref() != Some(owner) {
            return Err(conflict());
        }
        let result = self.write(owner, |tx| {
            tx.execute(
                "UPDATE wf_meta SET owner=NULL WHERE id=1 AND owner=?1",
                [owner],
            )
            .map_err(db_error)?;
            Ok(())
        });
        // No old connection may issue writes after relinquishing the OS lock.
        self.connection = None;
        self.owner = None;
        self.lock = None;
        result
    }
    pub fn read<T>(
        &mut self,
        action: impl FnOnce(&Transaction<'_>) -> Result<T, ForgeError>,
    ) -> Result<T, ForgeError> {
        let owner = self.current_owner()?;
        self.read_owned(&owner, action)
    }
    pub fn current_owner(&self) -> Result<String, ForgeError> {
        self.owner.clone().ok_or_else(conflict)
    }
    pub fn read_owned<T>(
        &mut self,
        owner: &str,
        action: impl FnOnce(&Transaction<'_>) -> Result<T, ForgeError>,
    ) -> Result<T, ForgeError> {
        if self.owner.as_deref() != Some(owner) || self.lock.is_none() {
            return Err(conflict());
        }
        let conn = self.connection.as_mut().ok_or_else(failed)?;
        let tx = conn.transaction().map_err(db_error)?;
        authorize(&tx, owner)?;
        let result = action(&tx)?;
        tx.commit().map_err(db_error)?;
        Ok(result)
    }
    pub fn write<T>(
        &mut self,
        owner: &str,
        action: impl FnOnce(&Transaction<'_>) -> Result<T, ForgeError>,
    ) -> Result<T, ForgeError> {
        if self.owner.as_deref() != Some(owner) || self.lock.is_none() {
            return Err(conflict());
        }
        let conn = self.connection.as_mut().ok_or_else(failed)?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        authorize(&tx, owner)?;
        let result = action(&tx)?;
        tx.commit().map_err(db_error)?;
        Ok(result)
    }
}
fn authorize(tx: &Transaction<'_>, owner: &str) -> Result<(), ForgeError> {
    let stored: Option<String> = tx
        .query_row("SELECT owner FROM wf_meta WHERE id=1", [], |r| r.get(0))
        .map_err(db_error)?;
    if stored.as_deref() == Some(owner) {
        Ok(())
    } else {
        Err(conflict())
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.connection = None;
        self.lock = None;
    }
}
