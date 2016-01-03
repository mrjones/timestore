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

use std::collections::HashMap;
use std::path::Path;
use std::vec::Vec;

pub type TimestampSeconds = i32;

pub struct TimeStore {
    databases: Box<HashMap<String, Box<Database<TimestampSeconds>>>>,
//    schema: Schema,
}

pub struct Schema {
    // TODO(mrjones): combine multiple series in a family for performance
    serieses: Vec<String>,
}

pub struct TSIterator<'a> {
    leveldb_iter: Box<leveldb::iterator::Iterator<'a, TimestampSeconds>>,
    limit: TimestampSeconds,
}

impl<'a> Iterator for TSIterator<'a> {
    type Item = (TimestampSeconds, Vec<u8>);

    fn next(&mut self) -> Option<(TimestampSeconds, Vec<u8>)> {
        if self.leveldb_iter.advance() &&
            (self.leveldb_iter.key() < self.limit) {
            return Some((self.leveldb_iter.key(), self.leveldb_iter.value()));
        } else {
            return None;
        }
    }
}

impl TimeStore {
    pub fn open<P: AsRef<Path>>(path: P, schema: Schema) -> Result<TimeStore, Error> {
        let mut databases : Box<HashMap<String, Box<Database<TimestampSeconds>>>> = Box::new(HashMap::new());

        for series in schema.serieses {
            let mut pathbuf = path.as_ref().to_path_buf();
            pathbuf.push(&series);

            let mut options = Options::new();
            options.create_if_missing = true;
            databases.insert(
                series.clone(),
                Box::new(try!(Database::open(pathbuf.as_path(), options))));
        }
          
        return Ok(TimeStore{
            databases: databases,
//            schema: schema,
        });
    }

    pub fn record(&mut self, series: &str, ts: TimestampSeconds, value: &[u8]) -> Result<(), Error> {
        let opts = WriteOptions::new();
        // TODO(mrjones): handle unknown series
        let database = self.databases.get(series).unwrap();
        return database.put(opts, ts, value);
    }

    pub fn lookup(&mut self, series: &str, ts: TimestampSeconds) -> Result<Option<Vec<u8>>, Error> {
        let opts = ReadOptions::new();
        // TODO(mrjones): handle unknown series
        let database = self.databases.get(series).unwrap();
        return database.get(opts, ts);
    }

    pub fn scan(&mut self, series: &str, start: TimestampSeconds, end: TimestampSeconds) -> Result<Vec<(i32, Vec<u8>)>, Error> {
        // TODO(mrjones): handle unknown series
        // TODO(mrjones): handle scanning multiple serieses
        let database = self.databases.get(series).unwrap();

        let opts = ReadOptions::new();
        let mut iter = database.iter(opts);
        iter.seek(&start);

        let mut result = Vec::new();
        while iter.advance() && iter.key() < end {
            result.push((iter.key(), iter.value()));
        }
        return Ok(result);
    }

    pub fn scan_iter(&mut self, series: &str, start: TimestampSeconds, end: TimestampSeconds) -> Result<TSIterator, Error> {
        // TODO(mrjones): handle unknown series
        let database = self.databases.get(series).unwrap();

        let opts = ReadOptions::new();
        let iter = Box::new(database.iter(opts));
        iter.seek(&start);
        

        return Ok(TSIterator{leveldb_iter: iter, limit: end});
    }
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use super::*;
    use self::tempdir::TempDir;

    fn simple_schema() -> Schema {
        return Schema{serieses: vec!["data".to_string()]};
    }

    #[test]
    fn record_then_lookup() {
        let tempdir = TempDir::new("record_then_lookup").unwrap();
        let mut ts = TimeStore::open(
            tempdir.path(), simple_schema())
            .expect("TimeStore::Open");
        ts.record("data", 12345, &[0, 1, 2, 3]).expect("record");
        assert_eq!(vec![0, 1, 2, 3], ts.lookup("data", 12345).unwrap().unwrap());
    }

    #[test]
    fn scan() {
        let tempdir = TempDir::new("scan").unwrap();
        let mut ts = TimeStore::open(
            tempdir.path(), simple_schema())
            .expect("TimeStore::Open");
        for i in 1..5 {
            ts.record("data", i, &[i as u8]).expect("record");
        }

        let res = ts.scan("data", 2, 4).expect("scan");
        assert_eq!(2, res.len());
        assert_eq!((2, vec![2]), res[0]);
        assert_eq!((3, vec![3]), res[1]);
    }

    #[test]
    fn scan_iter() {
        let tempdir = TempDir::new("scan_iter").unwrap();
        let mut ts = TimeStore::open(
            tempdir.path(), simple_schema())
            .expect("TimeStore::Open");
        for i in 1..5 {
            ts.record("data", i, &[i as u8]).expect("record");
        }

        let mut iter = ts.scan_iter("data", 2, 4).expect("scan");
        assert_eq!((2, vec![2]), iter.next().expect("unwrap 2"));
        assert_eq!((3, vec![3]), iter.next().expect("unwrap 2"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn multiple_series() {
        let schema = Schema{serieses: vec!["data1".to_string(), "data2".to_string()]};

        let tempdir = TempDir::new("record_then_lookup").unwrap();
        let mut ts = TimeStore::open(tempdir.path(), schema)
            .expect("TimeStore::Open");
        ts.record("data1", 12345, &[1, 1, 1, 1]).expect("record");
        ts.record("data2", 67890, &[2, 2, 2, 2]).expect("record");

        assert_eq!(vec![1, 1, 1, 1],
                   ts.lookup("data1", 12345).unwrap().unwrap());
        assert!(ts.lookup("data2", 12345).unwrap().is_none());

        assert!(ts.lookup("data1", 67890).unwrap().is_none());
        assert_eq!(vec![2, 2, 2, 2],
                   ts.lookup("data2", 67890).unwrap().unwrap());
    }
}
