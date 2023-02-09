use crate::error::Error;
use crate::keys::{key_doc, key_oid, key_state_vector, key_update, Key, OID};
use crate::{DocStore, KVEntry, KVStore};
use lmdb_rs::core::{CursorIterator, MdbResult, PageFull};
use lmdb_rs::{
    CursorKeyRangeIter, CursorValue, Database, DbHandle, Environment, MdbError, ReadonlyTransaction,
};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, TransactionMut, Update};

pub(crate) trait OptionalNotFound {
    type Return;
    type Error;

    fn optional(self) -> Result<Option<Self::Return>, Self::Error>;
}

impl<T> OptionalNotFound for MdbResult<T> {
    type Return = T;
    type Error = MdbError;

    /// Changes [MdbError::NotFound] onto [None] case.
    fn optional(self) -> Result<Option<Self::Return>, Self::Error> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(MdbError::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<'a> DocStore<'a> for Database<'a> {}

impl<'a> KVStore<'a> for Database<'a> {
    type Error = MdbError;
    type Cursor = LmdbRange<'a>;
    type Entry = LmdbEntry<'a>;

    fn get(&self, key: &[u8]) -> Result<Option<&'a [u8]>, Self::Error> {
        let value = self.get(&key).optional()?;
        Ok(value)
    }

    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<Option<&'a [u8]>, Self::Error> {
        let prev = self.remove(key)?;
        self.insert(&key, &value)?;
        Ok(prev)
    }

    fn remove(&self, key: &[u8]) -> Result<Option<&'a [u8]>, Self::Error> {
        let prev: Option<&[u8]> = self.get(&key).optional()?;
        if prev.is_some() {
            self.del(&key)?;
        }
        Ok(prev)
    }

    fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
        for v in self.keyrange(&from, &to)? {
            let key: &[u8] = v.get_key();
            if key > to {
                break; //TODO: for some reason key range doesn't always work
            }
            self.del(&key)?;
        }
        Ok(())
    }

    fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let from = from.to_vec();
        let to = to.to_vec();
        let cursor = unsafe { std::mem::transmute(self.keyrange(&from, &to)?) };
        Ok(LmdbRange { from, to, cursor })
    }
}

pub struct LmdbRange<'a> {
    from: Vec<u8>,
    to: Vec<u8>,
    cursor: CursorIterator<'a, CursorKeyRangeIter<'a>>,
}

impl<'a> Iterator for LmdbRange<'a> {
    type Item = LmdbEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let n = self.cursor.next()?;
        if n.get_key::<&[u8]>() > &self.to {
            None
        } else {
            Some(LmdbEntry(n))
        }
    }
}

pub struct LmdbEntry<'a>(CursorValue<'a>);

impl<'a> KVEntry<'a> for LmdbEntry<'a> {
    fn get_key(&self) -> &'a [u8] {
        self.0.get_key()
    }

    fn get_value(&self) -> &'a [u8] {
        self.0.get_value()
    }
}

pub(crate) struct OwnedCursorRange<'a> {
    txn: ReadonlyTransaction<'a>,
    db: Database<'a>,
    cursor: CursorIterator<'a, CursorKeyRangeIter<'a>>,
    start: Vec<u8>,
    end: Vec<u8>,
}

impl<'a> OwnedCursorRange<'a> {
    pub(crate) fn new<const N: usize>(
        txn: ReadonlyTransaction<'a>,
        db: Database<'a>,
        start: Key<N>,
        end: Key<N>,
    ) -> Result<Self, Error> {
        let start = start.into();
        let end = end.into();
        let cursor = unsafe { std::mem::transmute(db.keyrange(&start, &end)?) };

        Ok(OwnedCursorRange {
            txn,
            db,
            cursor,
            start,
            end,
        })
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }
}

impl<'a> Iterator for OwnedCursorRange<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.cursor.next()?;
        Some(v.get())
    }
}

#[cfg(test)]
mod test {
    use crate::DocStore;
    use lmdb_rs::core::DbCreate;
    use lmdb_rs::Environment;
    use std::sync::Arc;
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
        let cleaner = Cleaner::new("lmdb-create_get_remove");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();

        // insert document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, "hello");

            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);
            db.insert_doc("doc", &txn).unwrap();
            db_txn.commit().unwrap();
        }

        // retrieve document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            let db_txn = env.get_reader().unwrap();
            let db = db_txn.bind(&h);
            db.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello");

            let (sv, completed) = db.get_state_vector("doc").unwrap();
            assert_eq!(sv, Some(txn.state_vector()));
            assert!(completed);
        }

        // remove document
        {
            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);

            db.clear_doc("doc").unwrap();

            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "");

            let (sv, completed) = db.get_state_vector("doc").unwrap();
            assert!(sv.is_none());
            assert!(completed);
        }
    }
    #[test]
    fn multi_insert() {
        let cleaner = Cleaner::new("lmdb-multi_insert");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();

        // insert document twice
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello");

            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);

            db.insert_doc("doc", &txn).unwrap();

            text.push(&mut txn, " world");

            db.insert_doc("doc", &txn).unwrap();

            db_txn.commit().unwrap();
        }

        // retrieve document
        {
            let db_txn = env.get_reader().unwrap();
            let db = db_txn.bind(&h);

            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello world");
        }
    }

    #[test]
    fn incremental_updates() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("lmdb-incremental_updates");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();
        let env = Arc::new(env);
        let h = Arc::new(h);

        // store document updates
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");

            let env = env.clone();
            let h = h.clone();
            let _sub = doc.observe_update_v1(move |txn, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = db_txn.bind(&h);
                db.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
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

            let db_txn = env.get_reader().unwrap();
            let db = db_txn.bind(&h);
            db.load_doc(DOC_NAME, &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "abc");
        }

        // flush document
        {
            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);
            let doc = db.flush_doc(DOC_NAME).unwrap().unwrap();
            db_txn.commit().unwrap();

            let text = doc.get_or_insert_text("text");

            assert_eq!(text.get_string(&doc.transact()), "abc");
        }
    }

    #[test]
    fn state_vector_updates_only() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("lmdb-state_vector_updates_only");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();
        let env = Arc::new(env);
        let h = Arc::new(h);

        // store document updates
        let expected = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let env = env.clone();
            let h = h.clone();
            let _sub = doc.observe_update_v1(move |txn, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = db_txn.bind(&h);
                db.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            text.push(&mut doc.transact_mut(), "c");

            let sv = doc.transact().state_vector();
            sv
        };

        let db_txn = env.get_reader().unwrap();
        let db = db_txn.bind(&h);
        let (sv, completed) = db.get_state_vector(DOC_NAME).unwrap();
        assert!(sv.is_none());
        assert!(!completed); // since it's not completed, we should recalculate state vector from doc state
    }

    #[test]
    fn state_diff_from_updates() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("lmdb-state_diff_from_updates");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();
        let env = Arc::new(env);
        let h = Arc::new(h);

        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");

            let env = env.clone();
            let h = h.clone();
            let _sub = doc.observe_update_v1(move |txn, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = db_txn.bind(&h);
                db.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });

            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);
            (sv, update)
        };

        let db_txn = env.get_reader().unwrap();
        let db = db_txn.bind(&h);
        let actual = db.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn state_diff_from_doc() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("lmdb-state_diff_from_doc");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();

        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);

            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);
            db.insert_doc(DOC_NAME, &doc.transact()).unwrap();
            db_txn.commit().unwrap();

            (sv, update)
        };

        let db_txn = env.get_reader().unwrap();
        let db = db_txn.bind(&h);
        let actual = db.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn doc_meta() {
        const DOC_NAME: &str = "doc";
        let cleaner = Cleaner::new("lmdb-doc_meta");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();

        let db_txn = env.new_transaction().unwrap();
        let db = db_txn.bind(&h);
        let value = db.get_meta(DOC_NAME, "key".as_bytes()).unwrap();
        assert!(value.is_none());
        let prev = db
            .insert_meta(DOC_NAME, "key", "value1".as_bytes())
            .unwrap();
        db_txn.commit().unwrap();
        assert!(prev.is_none());

        let db_txn = env.new_transaction().unwrap();
        let db = db_txn.bind(&h);
        let prev = db
            .insert_meta(DOC_NAME, "key", "value2".as_bytes())
            .unwrap();
        db_txn.commit().unwrap();
        assert_eq!(prev.as_deref(), Some("value1".as_bytes()));

        let db_txn = env.new_transaction().unwrap();
        let db = db_txn.bind(&h);
        let prev = db.remove_meta(DOC_NAME, "key").unwrap();
        assert_eq!(prev.as_deref(), Some("value2".as_bytes()));
        let value = db.get_meta(DOC_NAME, "key").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn doc_meta_iter() {
        let cleaner = Cleaner::new("lmdb-doc_meta_iter");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();
        let db_txn = env.new_transaction().unwrap();
        let db = db_txn.bind(&h);

        db.insert_meta("A", "key1", "value1".as_bytes()).unwrap();
        db.insert_meta("B", "key2", "value2".as_bytes()).unwrap();
        db.insert_meta("B", "key3", "value3".as_bytes()).unwrap();
        db.insert_meta("C", "key4", "value1".as_bytes()).unwrap();

        let mut i = db.iter_meta("B").unwrap();
        assert_eq!(i.next(), Some(("key2".as_bytes(), "value2".as_bytes())));
        assert_eq!(i.next(), Some(("key3".as_bytes(), "value3".as_bytes())));
        assert!(i.next().is_none());
    }

    #[test]
    fn doc_iter() {
        let cleaner = Cleaner::new("lmdb-doc_iter");
        let env = init_env(cleaner.dir());
        let h = env.create_db("yrs", DbCreate).unwrap();
        let env = Arc::new(env);
        let h = Arc::new(h);

        // insert metadata
        {
            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);
            db.insert_meta("A", "key1", "value1".as_bytes()).unwrap();
            db_txn.commit().unwrap();
        }

        // insert full doc state
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello world");
            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);
            db.insert_doc("B", &txn).unwrap();
            db_txn.commit().unwrap();
        }

        // insert update
        {
            let doc = Doc::new();
            let env = env.clone();
            let h = h.clone();
            let sub = doc.observe_update_v1(move |txn, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = db_txn.bind(&h);
                db.push_update("C", &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello world");
        }

        {
            let db_txn = env.get_reader().unwrap();
            let db = db_txn.bind(&h);
            let mut i = db.iter_docs().unwrap();
            assert_eq!(i.next(), Some("A".as_bytes()));
            assert_eq!(i.next(), Some("B".as_bytes()));
            assert_eq!(i.next(), Some("C".as_bytes()));
            assert!(i.next().is_none());
        }

        // clear doc
        {
            let db_txn = env.new_transaction().unwrap();
            let db = db_txn.bind(&h);
            db.clear_doc("B").unwrap();
            db_txn.commit().unwrap();
        }

        {
            let db_txn = env.get_reader().unwrap();
            let db = db_txn.bind(&h);
            let mut i = db.iter_docs().unwrap();
            assert_eq!(i.next(), Some("A".as_bytes()));
            assert_eq!(i.next(), Some("C".as_bytes()));
            assert!(i.next().is_none());
        }
    }
}
