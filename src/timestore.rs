extern crate leveldb;

use self::leveldb::database::kv::KV;
use self::leveldb::database::Database;
use self::leveldb::error::Error;
use self::leveldb::options::Options;
use self::leveldb::options::ReadOptions;
use self::leveldb::options::WriteOptions;

use std::path;
use std::vec;

pub struct TimeStore {
    database: Box<Database<i32>>,
}

impl TimeStore {
    pub fn open<P: AsRef<path::Path>>(path: P) -> Result<TimeStore, Error> {
        let mut options = Options::new();
        options.create_if_missing = true;

        let db = try!(Database::open(path.as_ref(), options));
        return Ok(TimeStore{database: Box::new(db)});
    }

    pub fn record(&mut self, ts: i32, value: &[u8]) -> Result<(), Error> {
        let opts = WriteOptions::new();
        return self.database.put(opts, ts, value);
    }

    pub fn lookup(&mut self, ts: i32) -> Result<Option<vec::Vec<u8>>, Error> {
        let opts = ReadOptions::new();
        return self.database.get(opts, ts);
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use super::*;
    use self::tempdir::TempDir;

    #[test]
    fn record_then_lookup() {
        let tempdir = TempDir::new("record_then_lookup").unwrap();
        let mut ts = TimeStore::open(tempdir.path()).expect("TimeStore::Open");
        ts.record(12345, &[0, 1, 2, 3]).expect("record");
        assert_eq!(vec![0, 1, 2, 3], ts.lookup(12345).unwrap().unwrap());
    }
}
