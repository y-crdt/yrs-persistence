pub mod error;
pub mod keys;
mod lmdb;

use crate::error::Error;
use crate::keys::{
    key_doc, key_doc_end, key_doc_start, key_meta, key_oid, key_state_vector, key_update, Key, OID,
    V1,
};
use crate::lmdb::{
    delete_updates, get_oid, get_or_create_oid, load_doc, DatabaseExt, OptionalNotFound,
};
use lib0::any::Any;
use lmdb_rs::core::{CursorIterator, DbCreate, MdbResult};
use lmdb_rs::{
    Cursor, CursorKeyRangeIter, Database, DbFlags, DbHandle, Environment, MdbError, MdbValue,
    ReadonlyTransaction, ToMdbValue,
};
use std::borrow::Borrow;
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, TransactionMut, Update, WriteTxn};

pub struct LmdbPersistence {
    env: Environment,
    db_handle: DbHandle,
    options: Options,
}

impl LmdbPersistence {
    pub fn new(env: Environment) -> Result<Self, Error> {
        Self::open_with(env, Options::default())
    }

    pub fn open_with(env: Environment, options: Options) -> Result<Self, Error> {
        let db_handle = env.create_db(&options.db_name, DbCreate)?;
        Ok(LmdbPersistence {
            env,
            db_handle,
            options,
        })
    }

    /// Gets a [Doc] with a given name along side with its sequence of updates and merges them
    /// together, then stores them back. Clears all updates merged this way.
    pub fn flush_doc<K: AsRef<[u8]> + ?Sized>(&self, doc_name: &K) -> Result<Option<Doc>, Error> {
        let db_txn = self.env.new_transaction()?;
        let db = db_txn.bind(&self.db_handle);
        if let Some(oid) = get_oid(&db, doc_name.as_ref())? {
            let doc = Doc::new();
            let found = load_doc(&db, oid, &mut doc.transact_mut())?;
            if found & !(1 << 31) != 0 {
                // loaded doc was generated from updates
                let txn = doc.transact();
                let doc_state = txn.encode_state_as_update_v1(&StateVector::default());
                let state_vec = txn.state_vector().encode_v1();
                drop(txn);

                Self::insert_inner_v1(&db, oid, &doc_state, &state_vec)?;
                delete_updates(&db, oid)?;

                db_txn.commit()?;
                Ok(Some(doc))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Updates stored state of a document identified by `doc_name` via it's transaction.
    pub fn insert_doc<K: AsRef<[u8]> + ?Sized, T: ReadTxn>(
        &self,
        doc_name: &K,
        txn: &T,
    ) -> Result<(), Error> {
        let doc_state = txn.encode_diff_v1(&StateVector::default());
        let state_vector = txn.state_vector().encode_v1();
        self.insert_doc_raw_v1(doc_name.as_ref(), &doc_state, &state_vector)
    }

    pub fn insert_doc_raw_v1(
        &self,
        doc_name: &[u8],
        doc_state_v1: &[u8],
        doc_sv_v1: &[u8],
    ) -> Result<(), Error> {
        let db_txn = self.env.new_transaction()?;
        let db = db_txn.bind(&self.db_handle);
        let oid = get_or_create_oid(&db, doc_name)?;
        Self::insert_inner_v1(&db, oid, doc_state_v1, doc_sv_v1)?;
        db_txn.commit()?;
        Ok(())
    }

    fn insert_inner_v1(
        db: &Database,
        oid: OID,
        doc_state_v1: &[u8],
        doc_sv_v1: &[u8],
    ) -> Result<(), Error> {
        let key_doc = key_doc(oid);
        let key_sv = key_state_vector(oid);
        db.upsert(key_doc.as_ref(), &doc_state_v1)?;
        db.upsert(key_sv.as_ref(), &doc_sv_v1)?;
        Ok(())
    }

    /// Loads state of a current document identified by `doc_name` using provided read-write transaction.
    /// Returns true if [Doc] with given `doc_name` was found, false otherwise.
    pub fn load_doc<K: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K,
        txn: &mut TransactionMut,
    ) -> Result<bool, Error> {
        let db_txn = self.env.get_reader()?;
        let db = db_txn.bind(&self.db_handle);
        if let Some(oid) = get_oid(&db, doc_name.as_ref())? {
            let loaded = load_doc(&db, oid, txn)?;
            Ok(loaded != 0)
        } else {
            Ok(false)
        }
    }

    pub fn get_state_vector<K: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K,
    ) -> Result<Option<StateVector>, Error> {
        let db_txn = self.env.get_reader()?;
        let db = db_txn.bind(&self.db_handle);
        if let Some(oid) = get_oid(&db, doc_name.as_ref())? {
            let key = key_state_vector(oid);
            let data: Option<&[u8]> = db.get(&key).optional()?;
            let sv = if let Some(data) = data {
                let state_vector = StateVector::decode_v1(data)?;
                Some(state_vector)
            } else {
                None
            };
            let key = key_update(oid, 0);
            let mut cursor = db.new_cursor()?;
            let has_pending_updates = cursor.to_gte_key(&key).optional()?.is_some();
            drop(cursor);
            drop(db);
            drop(db_txn);
            if has_pending_updates {
                let doc = self.flush_doc(doc_name)?;
                if let Some(doc) = doc {
                    let sv = doc.transact().state_vector();
                    Ok(Some(sv))
                } else {
                    Ok(None)
                }
            } else {
                Ok(sv)
            }
        } else {
            Ok(None)
        }
    }

    pub fn push_update<K: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K,
        update: &[u8],
    ) -> Result<(), Error> {
        let db_txn = self.env.new_transaction()?;
        let db = db_txn.bind(&self.db_handle);
        let oid = get_or_create_oid(&db, doc_name.as_ref())?;
        let last_clock = {
            let start = key_update(oid, 0);
            let end = key_update(oid, u32::MAX);
            let iter = db.keyrange(&start, &end)?;
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
        db.insert(&update_key, &update)?;
        db_txn.commit()?;
        Ok(())
    }

    pub fn get_diff<K: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K,
        state_vector: &StateVector,
    ) -> Result<Option<Vec<u8>>, Error> {
        let doc = Doc::new();
        let found = {
            let mut txn = doc.transact_mut();
            self.load_doc(doc_name, &mut txn)?
        };
        if found {
            Ok(Some(doc.transact().encode_diff_v1(state_vector)))
        } else {
            Ok(None)
        }
    }

    pub fn clear_doc<K: AsRef<[u8]> + ?Sized>(&self, doc_name: &K) -> Result<(), Error> {
        let db_txn = self.env.new_transaction()?;
        let db = db_txn.bind(&self.db_handle);
        if let Some(oid) = db.try_del(key_oid(doc_name.as_ref()).as_ref())? {
            // all document related elements are stored within bounds [0,1,..oid,0]..[0,1,..oid,255]
            let oid: [u8; 4] = oid.try_into().unwrap();
            let oid = OID::from_be_bytes(oid);
            let start = key_doc_start(oid);
            let end = key_doc_end(oid);

            for v in db.keyrange(&start, &end)? {
                let key: &[u8] = v.get_key();
                db.del(&key)?;
            }

            db_txn.commit()?;
        }
        Ok(())
    }

    pub fn get_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K1,
        meta_key: &K2,
    ) -> Result<Option<&[u8]>, Error> {
        let db_txn = self.env.get_reader()?;
        let db = db_txn.bind(&self.db_handle);
        if let Some(oid) = get_oid(&db, doc_name.as_ref())? {
            let key = key_meta(oid, meta_key.as_ref());
            db.get(&key).optional()
        } else {
            Ok(None)
        }
    }

    pub fn insert_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K1,
        meta_key: &K2,
        meta: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        let db_txn = self.env.new_transaction()?;
        let db = db_txn.bind(&self.db_handle);
        let oid = get_or_create_oid(&db, doc_name.as_ref())?;
        let key = key_meta(oid, meta_key.as_ref());
        let prev = db.upsert(key.as_ref(), meta)?;
        let prev = prev.map(Vec::from);
        db_txn.commit()?;
        Ok(prev)
    }

    pub fn remove_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K1,
        meta_key: &K2,
    ) -> Result<Option<Vec<u8>>, Error> {
        let db_txn = self.env.new_transaction()?;
        let db = db_txn.bind(&self.db_handle);
        if let Some(oid) = get_oid(&db, doc_name.as_ref())? {
            let key = key_meta(oid, meta_key.as_ref());
            let prev = db.try_del(key.as_ref())?;
            let prev = prev.map(Vec::from);
            db_txn.commit()?;
            Ok(prev)
        } else {
            Ok(None)
        }
    }

    pub fn iter_docs(&self) -> Result<DocsNameIter, Error> {
        DocsNameIter::new(&self.env, &self.db_handle)
    }

    pub fn iter_state_vectors(&self) -> Result<StateVectorIter, Error> {
        StateVectorIter::new(&self.env, &self.db_handle)
    }

    pub fn iter_metadata(&self) -> MetadataIter {
        MetadataIter::new(&self.env, &self.db_handle)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Options {
    pub db_name: String,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            db_name: "yrs-db".to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct DocsNameIter<'a> {
    txn: ReadonlyTransaction<'a>,
    db: Database<'a>,
    cursor: CursorIterator<'a, CursorKeyRangeIter<'a>>,
}

impl<'a> DocsNameIter<'a> {
    fn new(env: &'a Environment, db_handle: &'a DbHandle) -> Result<Self, Error> {
        todo!()
    }
}

impl<'a> Iterator for DocsNameIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[derive(Debug)]
pub struct StateVectorIter<'a>(CursorIterator<'a, CursorKeyRangeIter<'a>>);

impl<'a> StateVectorIter<'a> {
    fn new(env: &'a Environment, db_handle: &'a DbHandle) -> Result<Self, Error> {
        //let txn = env.get_reader()?;
        //let db = txn.bind(db_handle);
        //let start = STATE_VECTOR_KEY_RANGE_START.as_ref();
        //let end = STATE_VECTOR_KEY_RANGE_END.as_ref();
        //let iter = db.keyrange(&start, &end)?;
        //Ok(StateVectorIter(iter))
        todo!()
    }
}

impl<'db> Iterator for StateVectorIter<'db> {
    type Item = Result<(&'db [u8], StateVector), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.0.next()?;
        let key: &[u8] = value.get_key(); // key pattern: 00{name:n}0
        let value: &[u8] = value.get_value();
        let doc_name = &key[2..(key.len() - 1)];
        match StateVector::decode_v1(value) {
            Ok(sv) => Some(Ok((doc_name, sv))),
            Err(err) => Some(Err(err.into())),
        }
    }
}

#[derive(Debug)]
pub struct MetadataIter<'a>(CursorIterator<'a, CursorKeyRangeIter<'a>>);

impl<'a> MetadataIter<'a> {
    fn new(env: &'a Environment, db_handle: &'a DbHandle) -> MetadataIter<'a> {
        todo!()
    }
}

impl<'a> Iterator for MetadataIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::{LmdbPersistence, Options};
    use lmdb_rs::Environment;
    use std::io;
    use std::sync::{Arc, Mutex};
    use yrs::{Doc, GetString, ReadTxn, Text, Transact};

    struct Cleaner(&'static str);

    impl Cleaner {
        fn new(dir: &'static str) -> Self {
            Self::cleanup(dir);
            Cleaner(dir)
        }

        fn dir(&self) -> &str {
            self.0
        }

        fn cleanup(dir: &str) {
            if let Err(_) = std::fs::remove_dir_all(dir) {
                // if dir doesn't exists, ignore
            }
        }
    }

    impl Drop for Cleaner {
        fn drop(&mut self) {
            Self::cleanup(self.dir());
        }
    }

    fn init_env(dir: &str) -> Environment {
        let env = Environment::new()
            .autocreate_dir(true)
            .max_dbs(4)
            .open(dir, 0o777)
            .unwrap();
        env
    }

    #[test]
    fn create_get_remove() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("test-create_get_remove");
        let env = init_env(cleaner.dir());
        let db = LmdbPersistence::new(env).unwrap();

        // insert document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, "hello");

            db.insert_doc(DOC_NAME, &txn).unwrap();
        }

        // retrieve document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db.load_doc(DOC_NAME, &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello");

            let sv = db.get_state_vector(DOC_NAME.as_bytes()).unwrap();
            assert_eq!(sv, Some(txn.state_vector()));
        }

        // remove document
        {
            db.clear_doc(DOC_NAME.as_bytes()).unwrap();

            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db.load_doc(DOC_NAME, &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "");

            let sv = db.get_state_vector(DOC_NAME).unwrap();
            assert!(sv.is_none());
        }
    }
    #[test]
    fn multi_insert() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("test-multi_insert");
        let env = init_env(cleaner.dir());
        let db = LmdbPersistence::new(env).unwrap();

        // insert document twice
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello");

            db.insert_doc(DOC_NAME, &txn).unwrap();

            text.push(&mut txn, " world");

            db.insert_doc(DOC_NAME, &txn).unwrap();
        }

        // retrieve document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db.load_doc(DOC_NAME, &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello world");
        }
    }

    #[test]
    fn incremental_updates() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("test-incremental_updates");
        let env = init_env(cleaner.dir());
        let db = Arc::new(Mutex::new(LmdbPersistence::new(env).unwrap()));

        // store document updates
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let db_copy = db.clone();
            let _sub = doc.observe_update_v1(move |txn, u| {
                let handle = db_copy.lock().unwrap();
                handle.push_update(DOC_NAME, &u.update).unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            text.push(&mut doc.transact_mut(), "c");
        }

        // load document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            let handle = db.lock().unwrap();
            handle.load_doc(DOC_NAME, &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "abc");
        }

        // flush document
        {
            let handle = db.lock().unwrap();
            let doc = handle.flush_doc(DOC_NAME.as_bytes()).unwrap().unwrap();
            let text = doc.get_or_insert_text("text");

            assert_eq!(text.get_string(&doc.transact()), "abc");
        }
    }

    #[test]
    fn state_vector_updates_only() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("test-state_vector_updates_only");
        let env = init_env(cleaner.dir());
        let db = Arc::new(Mutex::new(LmdbPersistence::new(env).unwrap()));

        // store document updates
        let expected = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let db_copy = db.clone();
            let _sub = doc.observe_update_v1(move |txn, u| {
                let handle = db_copy.lock().unwrap();
                handle.push_update(DOC_NAME, &u.update).unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            text.push(&mut doc.transact_mut(), "c");

            let sv = doc.transact().state_vector();
            sv
        };

        let handle = db.lock().unwrap();
        let sv = handle.get_state_vector(DOC_NAME.as_bytes()).unwrap();
        assert_eq!(sv, Some(expected));
    }

    #[test]
    fn state_diff_from_updates() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("test-state_diff_from_updates");
        let env = init_env(cleaner.dir());
        let db = Arc::new(Mutex::new(LmdbPersistence::new(env).unwrap()));
        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let db_copy = db.clone();
            let _sub = doc.observe_update_v1(move |txn, u| {
                let handle = db_copy.lock().unwrap();
                handle.push_update(DOC_NAME, &u.update).unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);
            (sv, update)
        };

        let handle = db.lock().unwrap();
        let actual = handle.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn state_diff_from_doc() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("test-state_diff_from_doc");
        let env = init_env(cleaner.dir());
        let db = LmdbPersistence::new(env).unwrap();
        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);
            db.insert_doc(DOC_NAME, &doc.transact()).unwrap();
            (sv, update)
        };

        let actual = db.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn doc_meta() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("test-doc_meta");
        let env = init_env(cleaner.dir());
        let db = LmdbPersistence::new(env).unwrap();
        let value = db.get_meta(DOC_NAME, "key".as_bytes()).unwrap();
        assert!(value.is_none());
        let prev = db
            .insert_meta(DOC_NAME, "key", "value1".as_bytes())
            .unwrap();
        assert!(prev.is_none());
        let prev = db
            .insert_meta(DOC_NAME, "key", "value2".as_bytes())
            .unwrap();
        assert_eq!(prev.as_deref(), Some("value1".as_bytes()));
        let prev = db.remove_meta(DOC_NAME, "key").unwrap();
        assert_eq!(prev.as_deref(), Some("value2".as_bytes()));
        let value = db.get_meta(DOC_NAME, "key").unwrap();
        assert!(value.is_none());
    }
}
