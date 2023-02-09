pub mod error;
pub mod keys;
mod lmdb;

use crate::error::Error;
use crate::keys::{
    doc_oid_name, key_doc, key_doc_end, key_doc_start, key_meta, key_meta_end, key_meta_start,
    key_oid, key_state_vector, key_update, Key, KEYSPACE_DOC, KEYSPACE_OID, OID, V1,
};
use lib0::any::Any;
use lmdb_rs::{Database, ReadonlyTransaction};
use std::marker::PhantomData;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, TransactionMut, Update, WriteTxn};

pub trait KVStore<'a> {
    type Error: std::error::Error;
    type Cursor: Iterator<Item = Self::Entry>;
    type Entry: KVEntry<'a>;

    /// Return a value stored under given `key` or `None` if key was not found.
    fn get(&self, key: &[u8]) -> Result<Option<&'a [u8]>, Self::Error>;

    /// Insert a new `value` under given `key` or replace an existing value with new one if
    /// entry with that `key` already existed.
    ///
    /// Return previously stored value, if entry existed before.
    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<Option<&'a [u8]>, Self::Error>;

    /// Return a value stored under the given `key` if it exists.
    /// Return previously stored value, if entry existed before.
    fn remove(&self, key: &[u8]) -> Result<Option<&'a [u8]>, Self::Error>;

    /// Remove all keys between `from`..=`to` range of keys.
    fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error>;

    /// Return an iterator over all entries between `from`..=`to` range of keys.
    fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error>;
}

pub trait KVEntry<'a> {
    /// Returns the key stored under a current entry.
    fn get_key(&self) -> &'a [u8];

    /// Returns the value stored under a current entry.
    fn get_value(&self) -> &'a [u8];
}

pub trait DocStore<'a>: KVStore<'a> + Sized
where
    Error: From<<Self as KVStore<'a>>::Error>,
{
    fn insert_doc<K: AsRef<[u8]> + ?Sized, T: ReadTxn>(
        &self,
        name: &K,
        txn: &T,
    ) -> Result<(), Error> {
        let doc_state = txn.encode_diff_v1(&StateVector::default());
        let state_vector = txn.state_vector().encode_v1();
        self.insert_doc_raw_v1(name.as_ref(), &doc_state, &state_vector)
    }

    fn insert_doc_raw_v1(
        &self,
        name: &[u8],
        doc_state_v1: &[u8],
        doc_sv_v1: &[u8],
    ) -> Result<(), Error> {
        let oid = get_or_create_oid(self, name)?;
        insert_inner_v1(self, oid, doc_state_v1, doc_sv_v1)?;
        Ok(())
    }

    fn load_doc<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        txn: &mut TransactionMut,
    ) -> Result<bool, Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let loaded = load_doc(self, oid, txn)?;
            Ok(loaded != 0)
        } else {
            Ok(false)
        }
    }

    fn flush_doc<K: AsRef<[u8]> + ?Sized>(&self, name: &K) -> Result<Option<Doc>, Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let doc = flush_doc(self, oid)?;
            Ok(doc)
        } else {
            Ok(None)
        }
    }

    fn get_state_vector<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
    ) -> Result<(Option<StateVector>, bool), Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let key = key_state_vector(oid);
            let data = self.get(&key)?;
            let sv = if let Some(data) = data {
                let state_vector = StateVector::decode_v1(data)?;
                Some(state_vector)
            } else {
                None
            };
            let update_range_start = key_update(oid, 0);
            let update_range_end = key_update(oid, u32::MAX);
            let mut iter = self.iter_range(&update_range_start, &update_range_end)?;
            let up_to_date = iter.next().is_none();
            Ok((sv, up_to_date))
        } else {
            Ok((None, true))
        }
    }

    fn push_update<K: AsRef<[u8]> + ?Sized>(&self, name: &K, update: &[u8]) -> Result<(), Error> {
        let oid = get_or_create_oid(self, name.as_ref())?;
        let last_clock = {
            let start = key_update(oid, 0);
            let end = key_update(oid, u32::MAX);
            let iter = self.iter_range(&start, &end)?;
            if let Some(last) = iter.last() {
                let last_key: &[u8] = last.get_key();
                let len = last_key.len();
                let last_clock = &last_key[(len - 5)..(len - 1)]; // update key scheme: 01{name:n}1{clock:4}0
                u32::from_be_bytes(last_clock.try_into().unwrap())
            } else {
                0
            }
        };
        let update_key = key_update(oid, last_clock + 1);
        self.upsert(&update_key, &update)?;
        Ok(())
    }

    fn get_diff<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        sv: &StateVector,
    ) -> Result<Option<Vec<u8>>, Error> {
        let doc = Doc::new();
        let found = {
            let mut txn = doc.transact_mut();
            self.load_doc(name, &mut txn)?
        };
        if found {
            Ok(Some(doc.transact().encode_diff_v1(sv)))
        } else {
            Ok(None)
        }
    }

    fn clear_doc<K: AsRef<[u8]> + ?Sized>(&self, name: &K) -> Result<(), Error> {
        let oid_key = key_oid(name.as_ref());
        if let Some(oid) = self.remove(&oid_key)? {
            // all document related elements are stored within bounds [0,1,..oid,0]..[0,1,..oid,255]
            let oid: [u8; 4] = oid.try_into().unwrap();
            let oid = OID::from_be_bytes(oid);
            let start = key_doc_start(oid);
            let end = key_doc_end(oid);
            for v in self.iter_range(&start, &end)? {
                let key: &[u8] = v.get_key();
                if key > &end {
                    break; //TODO: for some reason key range doesn't always work
                }
                self.remove(&key)?;
            }
        }
        Ok(())
    }

    fn get_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
    ) -> Result<Option<&'a [u8]>, Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let key = key_meta(oid, meta_key.as_ref());
            Ok(self.get(&key)?)
        } else {
            Ok(None)
        }
    }

    fn insert_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
        meta: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        let oid = get_or_create_oid(self, name.as_ref())?;
        let key = key_meta(oid, meta_key.as_ref());
        let prev = self.upsert(&key, meta)?;
        let prev = prev.map(Vec::from);
        Ok(prev)
    }

    fn remove_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
    ) -> Result<Option<Vec<u8>>, Error> {
        if let Some(oid) = get_oid(self, name.as_ref())? {
            let key = key_meta(oid, meta_key.as_ref());
            let prev = self.remove(&key)?;
            let prev = prev.map(Vec::from);
            Ok(prev)
        } else {
            Ok(None)
        }
    }

    fn iter_docs(&self) -> Result<DocsNameIter<'a, Self::Cursor, Self::Entry>, Error> {
        let start = Key::from_const([V1, KEYSPACE_OID]);
        let end = Key::from_const([V1, KEYSPACE_DOC]);
        let cursor = self.iter_range(&start, &end)?;
        Ok(DocsNameIter {
            cursor,
            start,
            end,
            _a: PhantomData::default(),
        })
    }

    fn iter_meta<K: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K,
    ) -> Result<MetadataIter<'a, Self::Cursor, Self::Entry>, Error> {
        if let Some(oid) = get_oid(self, doc_name.as_ref())? {
            let start = key_meta_start(oid).to_vec();
            let end = key_meta_end(oid).to_vec();
            let cursor = self.iter_range(&start, &end)?;
            Ok(MetadataIter(Some((
                cursor,
                start,
                end,
                PhantomData::default(),
            ))))
        } else {
            Ok(MetadataIter(None))
        }
    }
}

fn get_oid<'a, DB: DocStore<'a> + ?Sized>(db: &DB, name: &[u8]) -> Result<Option<OID>, Error>
where
    error::Error: From<<DB as KVStore<'a>>::Error>,
{
    let key = key_oid(name);
    let value = db.get(&key)?;
    if let Some(value) = value {
        let value: [u8; 4] = value.try_into().unwrap();
        Ok(Some(OID::from_be_bytes(value)))
    } else {
        Ok(None)
    }
}

fn get_or_create_oid<'a, DB: DocStore<'a> + ?Sized>(db: &DB, name: &[u8]) -> Result<OID, Error>
where
    error::Error: From<<DB as KVStore<'a>>::Error>,
{
    if let Some(oid) = get_oid(db, name)? {
        Ok(oid)
    } else {
        /*
           Since pattern is:

           00{doc_name:n}0      - OID key pattern
           01{oid:4}0           - document key pattern

           Use 00{0000}0 to try to move cursor to GTE first document, then move cursor 1 position
           back to get the latest OID or not found.
        */
        let mut cursor = db.iter_range([V1, KEYSPACE_OID].as_ref(), [V1, KEYSPACE_DOC].as_ref())?;
        let last_oid = if let Some(last) = cursor.last() {
            let value = last.get_value();
            let last_value = OID::from_be_bytes(value.try_into().unwrap());
            last_value
        } else {
            0
        };
        let new_oid = last_oid + 1;
        let key = key_oid(name);
        db.upsert(&key, new_oid.to_be_bytes().as_ref())?;
        Ok(new_oid)
    }
}

fn load_doc<'a, DB: DocStore<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    txn: &mut TransactionMut,
) -> Result<u32, Error>
where
    error::Error: From<<DB as KVStore<'a>>::Error>,
{
    let mut found = false;
    {
        let doc_key = key_doc(oid);
        if let Some(doc_state) = db.get(&doc_key)? {
            let update = Update::decode_v1(doc_state)?;
            txn.apply_update(update);
            found = true;
        }
    }
    let mut update_count = 0;
    {
        let update_key_start = key_update(oid, 0);
        let update_key_end = key_update(oid, u32::MAX);
        let mut iter = db.iter_range(&update_key_start, &update_key_end)?;
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

fn delete_updates<'a, DB: DocStore<'a> + ?Sized>(db: &DB, oid: OID) -> Result<(), Error>
where
    error::Error: From<<DB as KVStore<'a>>::Error>,
{
    let start = key_update(oid, 0);
    let end = key_update(oid, u32::MAX);
    db.remove_range(&start, &end)?;
    Ok(())
}

fn flush_doc<'a, DB: DocStore<'a> + ?Sized>(db: &DB, oid: OID) -> Result<Option<Doc>, Error>
where
    error::Error: From<<DB as KVStore<'a>>::Error>,
{
    let doc = Doc::new();
    let found = load_doc(db, oid, &mut doc.transact_mut())?;
    if found & !(1 << 31) != 0 {
        // loaded doc was generated from updates
        let txn = doc.transact();
        let doc_state = txn.encode_state_as_update_v1(&StateVector::default());
        let state_vec = txn.state_vector().encode_v1();
        drop(txn);

        insert_inner_v1(db, oid, &doc_state, &state_vec)?;
        delete_updates(db, oid)?;
        Ok(Some(doc))
    } else {
        Ok(None)
    }
}

fn insert_inner_v1<'a, DB: DocStore<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    doc_state_v1: &[u8],
    doc_sv_v1: &[u8],
) -> Result<(), Error>
where
    error::Error: From<<DB as KVStore<'a>>::Error>,
{
    let key_doc = key_doc(oid);
    let key_sv = key_state_vector(oid);
    db.upsert(&key_doc, doc_state_v1)?;
    db.upsert(&key_sv, doc_sv_v1)?;
    Ok(())
}

pub struct DocsNameIter<'a, I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry<'a>,
{
    cursor: I,
    start: Key<2>,
    end: Key<2>,
    _a: PhantomData<&'a ()>,
}

impl<'a, I, E> Iterator for DocsNameIter<'a, I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry<'a>,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.cursor.next()?;
        let key = v.get_key();
        Some(doc_oid_name(key))
    }
}

pub struct MetadataIter<'a, I, E>(Option<(I, Vec<u8>, Vec<u8>, PhantomData<&'a ()>)>)
where
    I: Iterator<Item = E>,
    E: KVEntry<'a>;

impl<'a, I, E> Iterator for MetadataIter<'a, I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry<'a>,
{
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (cursor, _, _, _) = self.0.as_mut()?;
        let v = cursor.next()?;
        let key = v.get_key();
        let value = v.get_value();
        let meta_key = &key[7..key.len() - 1];
        Some((meta_key, value))
    }
}
