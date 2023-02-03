use crate::error::Error;
use crate::keys::{key_doc, key_oid, key_update, Key, OID};
use lmdb_rs::core::{MdbResult, PageFull};
use lmdb_rs::{Database, MdbError};
use yrs::updates::decoder::Decode;
use yrs::{TransactionMut, Update};

pub(crate) trait OptionalNotFound {
    type Return;

    fn optional(self) -> Result<Option<Self::Return>, Error>;
}

impl<T> OptionalNotFound for MdbResult<T> {
    type Return = T;

    /// Changes [MdbError::NotFound] onto [None] case.
    fn optional(self) -> Result<Option<Self::Return>, Error> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(MdbError::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

pub(crate) trait DatabaseExt {
    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<Option<&[u8]>, Error>;
    fn try_del(&self, key: &[u8]) -> Result<Option<&[u8]>, Error>;
}

impl<'a> DatabaseExt for Database<'a> {
    /// Insert a new `value` under given `key` or replace an existing value with new one if
    /// entry with that `key` already existed.
    ///
    /// Return previously stored value, if entry existed before.
    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<Option<&[u8]>, Error> {
        let prev = self.try_del(key)?;
        self.insert(&key, &value)?;
        Ok(prev)
    }

    fn try_del(&self, key: &[u8]) -> Result<Option<&[u8]>, Error> {
        let prev: Option<&[u8]> = self.get(&key).optional()?;
        if prev.is_some() {
            self.del(&key)?;
        }
        Ok(prev)
    }
}

pub(crate) fn get_oid(db: &Database, doc_name: &[u8]) -> Result<Option<OID>, Error> {
    let key = key_oid(doc_name);
    let value: Option<&[u8]> = db.get(&key).optional()?;
    if let Some(value) = value {
        let value: [u8; 4] = value.try_into().unwrap();
        Ok(Some(OID::from_be_bytes(value)))
    } else {
        Ok(None)
    }
}

pub(crate) fn get_or_create_oid(db: &Database, doc_name: &[u8]) -> Result<OID, Error> {
    if let Some(oid) = get_oid(db, doc_name)? {
        Ok(oid)
    } else {
        /*
           Since pattern is:

           00{doc_name:n}0      - OID key pattern
           01{oid:4}0           - document key pattern

           Use 00{0000}0 to try to move cursor to GTE first document, then move cursor 1 position
           back to get the latest OID or not found.
        */
        let key = [0, 1, 0, 0, 0, 0, 0].as_ref();
        let mut cursor = db.new_cursor()?;
        let found = cursor.to_gte_key(&key).optional()?.is_some();
        let last_oid = if found {
            cursor.to_prev_key()?;
            let value: &[u8] = cursor.get_value()?;
            let last_value = OID::from_be_bytes(value.try_into().unwrap());
            last_value
        } else {
            0
        };
        let new_oid = last_oid + 1;
        let key = key_oid(doc_name);
        db.insert(&key, &new_oid.to_be_bytes().as_ref())?;
        Ok(new_oid)
    }
}

pub(crate) fn load_doc(db: &Database, oid: OID, txn: &mut TransactionMut) -> Result<u32, Error> {
    let mut found = false;
    {
        let doc_key = key_doc(oid);
        let doc_state: Option<&[u8]> = db.get(&doc_key).optional()?;
        if let Some(doc_state) = doc_state {
            let update = Update::decode_v1(doc_state)?;
            txn.apply_update(update);
            found = true;
        }
    }
    let mut update_count = 0;
    {
        let update_key_start = key_update(oid, 0);
        let update_key_end = key_update(oid, u32::MAX);
        let mut iter = db.keyrange(&update_key_start, &update_key_end)?;
        while let Some(e) = iter.next() {
            let value: &[u8] = e.get_value();
            let update = Update::decode_v1(value)?;
            txn.apply_update(update);
            update_count += 1;
        }
    }
    if found {
        update_count |= 1 << 31; // mark hi bit to note that document core state was used
    }
    Ok(update_count)
}

pub(crate) fn delete_updates(db: &Database, oid: OID) -> Result<usize, Error> {
    let start = key_update(oid, 0);
    let end = key_update(oid, u32::MAX);
    let mut count = 0;
    let mut cursor = db.new_cursor()?;
    if cursor.to_gte_key(&start).optional()?.is_some() {
        while cursor.to_next_key().optional()?.is_some() {
            let key: &[u8] = cursor.get_key()?;
            if key > end.as_ref() {
                break;
            }
            db.del(&key)?;
        }
    }
    Ok(count)
}
