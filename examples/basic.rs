use lmdb_rs::core::DbCreate;
use lmdb_rs::Environment;

fn main() {
    let env = Environment::new()
        .max_dbs(1)
        .open("test.db", 0o777)
        .unwrap();
    let handle = env.create_db("yrs", DbCreate).unwrap();

    {
        let txn = env.new_transaction().unwrap();
        let db = txn.bind(&handle);

        let pairs = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];

        for &(k, v) in pairs.iter() {
            db.set(&k, &v).unwrap();
        }

        txn.commit().unwrap();
    }

    let txn = env.get_reader().unwrap();
    let db = txn.bind(&handle);
    let mut c = db.new_cursor().unwrap();
    c.to_gte_key(&"key1").unwrap();

    let key: String = c.get_key().unwrap();
    let value: String = c.get_value().unwrap();
    println!("{}={}", key, value);

    c.to_next_key().unwrap();

    let key: String = c.get_key().unwrap();
    let value: String = c.get_value().unwrap();
    println!("{}={}", key, value);
}
