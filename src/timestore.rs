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
    series_to_family_map: HashMap<String, String>,
}

pub struct Schema {
    schema: HashMap<String, Vec<String>>,
}

impl TimeStore {
    pub fn open<P: AsRef<Path>>(path: P, schema: Schema) -> Result<TimeStore, Error> {
        let mut databases : Box<HashMap<String, Box<Database<TimestampSeconds>>>> = Box::new(HashMap::new());
        let mut series_to_family_map = HashMap::new();

        for (family, serieses) in &schema.schema {
            for series in serieses {
                series_to_family_map.insert(series.clone(), family.clone());
            }

            let mut options = Options::new();
            options.create_if_missing = true;
            databases.insert(
                family.clone(),
                Box::new(try!(Database::open(path.as_ref(),options))));
        }

        return Ok(TimeStore{
            databases: databases,
//            schema: schema,
            series_to_family_map: series_to_family_map.clone(),
        });
    }

    pub fn record(&mut self, series: String, ts: TimestampSeconds, value: &[u8]) -> Result<(), Error> {
        let opts = WriteOptions::new();
        // TODO(mrjones): handle unknown family
        let family = self.series_to_family_map.get(&series).unwrap();
        let database = self.databases.get(family).unwrap();
        return database.put(opts, ts, value);
    }

    pub fn lookup(&mut self, series: String, ts: TimestampSeconds) -> Result<Option<Vec<u8>>, Error> {
        let opts = ReadOptions::new();
        // TODO(mrjones): handle unknown family
        let family = self.series_to_family_map.get(&series).unwrap();
        let database = self.databases.get(family).unwrap();
        return database.get(opts, ts);
    }

    pub fn scan(&mut self, series: String, start: TimestampSeconds, end: TimestampSeconds) -> Result<Vec<(i32, Vec<u8>)>, Error> {
        // TODO(mrjones): handle unknown family
        // TODO(mrjones): handle scanning multiple serieses
        let family = self.series_to_family_map.get(&series).unwrap();
        let database = self.databases.get(family).unwrap();

        let opts = ReadOptions::new();
        let mut iter = database.iter(opts);
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

    use std::collections::HashMap;

    fn simple_schema() -> Schema {
        let mut map = HashMap::new();
        map.insert("family".to_string(), vec!["data".to_string()]);
        return Schema{schema: map};
    }

    #[test]
    fn record_then_lookup() {
        let tempdir = TempDir::new("record_then_lookup").unwrap();
        let mut ts = TimeStore::open(
            tempdir.path(), simple_schema())
            .expect("TimeStore::Open");
        ts.record("data".to_string(), 12345, &[0, 1, 2, 3]).expect("record");
        assert_eq!(vec![0, 1, 2, 3], ts.lookup("data".to_string(), 12345).unwrap().unwrap());
    }

    #[test]
    fn scan() {
        let tempdir = TempDir::new("scan").unwrap();
        let mut ts = TimeStore::open(
            tempdir.path(), simple_schema())
            .expect("TimeStore::Open");
        for i in 1..5 {
            ts.record("data".to_string(), i, &[i as u8]).expect("record");
        }

        let res = ts.scan("data".to_string(), 2, 4).expect("scan");
        assert_eq!(2, res.len());
        assert_eq!((2, vec![2]), res[0]);
        assert_eq!((3, vec![3]), res[1]);
    }

    #[test]
    fn multiple_series() {
        let mut map = HashMap::new();
        map.insert("family".to_string(),
                   vec!["data1".to_string(), "data2".to_string()]);
        let schema = Schema{schema: map};

        let tempdir = TempDir::new("record_then_lookup").unwrap();
        let mut ts = TimeStore::open(
            tempdir.path(), schema)
            .expect("TimeStore::Open");
        ts.record("data1".to_string(), 12345, &[0, 1, 2, 3]).expect("record");
        assert_eq!(vec![0, 1, 2, 3],
                   ts.lookup("data1".to_string(), 12345).unwrap().unwrap());
        // TODO(mrjones): these series shouldn't overlap
        assert!(ts.lookup("data2".to_string(), 12345).unwrap().is_none());
    }
}
