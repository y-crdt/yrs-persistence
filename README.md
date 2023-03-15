# Yrs backend for persistent key-value stores

This repository contains code of 3 crates: 

- `yrs-kvstore`: a generic library that adds a bunch of utility functions that simplify process of persisting and managing Yrs/Yjs document contents. Since it's generic, it's capabilities can be applied to basically any modern persistent key-value store.
- `yrs-lmdb`: an [LMDB](http://www.lmdb.tech/doc/) implementation of `yrs-kvstore`.
- `yrs-rocksdb`: a [RocksDB](https://rocksdb.org/) implementation of `yrs-kvstore`.

## Sponsors

[![NLNET](https://nlnet.nl/image/logo_nlnet.svg)](https://nlnet.nl/)
