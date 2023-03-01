# yrs-kvstore

yrs-kvstore is a generic library that allows to quickly implement
[Yrs](https://docs.rs/yrs/latest/yrs/index.html) specific document operations over any kind
of persistent key-value store i.e. LMDB or RocksDB.
In order to do so, persistent unit of transaction should define set of basic operations via
`KVStore` trait implementation. Once this is done it can implement `DocOps`. Latter offers a
set of useful operations like document metadata management options, document and update merging
etc. They are implemented automatically as long struct has correctly implemented `KVStore`.