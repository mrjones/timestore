extern crate leveldb;
extern crate db_key;

use self::leveldb::database::kv::KV;
use self::leveldb::database::Database;
use self::leveldb::error::Error;
use self::leveldb::iterator::Iterable;
use self::leveldb::iterator::LevelDBIterator;
use self::leveldb::options::Options;
use self::leveldb::options::ReadOptions;
use self::leveldb::options::WriteOptions;

use std::path::Path;
use std::vec::Vec;

pub type TimestampSeconds = i32;

pub struct TimeStore {
    database: Box<Database<TimestampSeconds>>,
}

impl TimeStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<TimeStore, Error> {
        let mut options = Options::new();
        options.create_if_missing = true;

        let db = try!(Database::open(path.as_ref(), options));
        return Ok(TimeStore{database: Box::new(db)});
    }

    pub fn record(&mut self, ts: TimestampSeconds, value: &[u8]) -> Result<(), Error> {
        let opts = WriteOptions::new();
        return self.database.put(opts, ts, value);
    }

    pub fn lookup(&mut self, ts: TimestampSeconds) -> Result<Option<Vec<u8>>, Error> {
        let opts = ReadOptions::new();
        return self.database.get(opts, ts);
    }

    pub fn scan(&mut self, start: TimestampSeconds, end: TimestampSeconds) -> Result<Vec<(i32, Vec<u8>)>, Error> {
        let opts = ReadOptions::new();
        let mut iter = self.database.iter(opts);
        iter.seek(&start);

        let mut result = Vec::new();
        while iter.advance() && iter.key() < end {
            result.push((iter.key(), iter.value()));
        }
        return Ok(result);
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

    #[test]
    fn scan() {
        let tempdir = TempDir::new("scan").unwrap();
        let mut ts = TimeStore::open(tempdir.path()).expect("TimeStore::Open");
        for i in 1..5 {
            ts.record(i, &[i as u8]).expect("record");
        }

        let res = ts.scan(2, 4).expect("scan");
        assert_eq!(2, res.len());
        assert_eq!((2, vec![2]), res[0]);
        assert_eq!((3, vec![3]), res[1]);
    }
}
