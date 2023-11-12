#[macro_use]
extern crate serde_derive;
extern crate byteorder;
extern crate crc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use log::{info, log_enabled, Level};
use serde_derive::{Deserialize, Serialize};
use std::panic;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};
use timed::timed;
pub type ByteString = Vec<u8>;
pub type ByteStr = [u8];
const INDEX_KEY: &ByteStr = b"+index";

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct ActionKV {
    file_: File,
    index_: File,
    pub index: HashMap<ByteString, u64>,
}

/*
    THIS IS BITCASK FILE FORMAT
    checksum | key_len | value_len |     key      |     value
    [u32;1]    [u32;1]   [u32;1]     [u8;key_len]   [u8;value_len]
*/
impl ActionKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        if !std::path::Path::new(&path).exists() {
            std::fs::create_dir(path)?;
        }
        let file_ = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path.join("data"))?;
        let index_ = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join("index"))?;
        let index = HashMap::new();
        Ok(ActionKV {
            file_,
            index_,
            index,
        })
    }
    fn process_records<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let value_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + value_len;
        let mut data = ByteString::with_capacity(data_len as usize);
        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        };
        debug_assert_eq!(data_len as usize, data.len());
        let checksum = crc32::checksum_ieee(&data);
        if checksum != saved_checksum {
            panic!(
                "Data corruption encountered {:08x} != {:08x}",
                checksum, saved_checksum
            )
        };
        let value = data.split_off(key_len as usize);
        let key = data;
        Ok(KeyValuePair { key, value })
    }
    fn store_index_on_disk(&mut self, index_key: &ByteStr) -> io::Result<()> {
        self.index.remove(index_key);
        let index_as_bytes = bincode::serialize(&self.index).unwrap();
        self.index = std::collections::HashMap::new();
        self.insert_(index_key, &index_as_bytes, true)?;
        Ok(())
    }
    fn insert_(&mut self, key: &ByteStr, value: &ByteStr, saving_index: bool) -> io::Result<()> {
        let mut f = BufWriter::new(&mut self.file_);
        if saving_index == true {
            f = BufWriter::new(&mut self.index_);
        }
        let key_len = key.as_ref().len();
        let value_len = value.as_ref().len();
        let mut tmp = ByteString::with_capacity(key_len + value_len);
        tmp.extend(key);
        tmp.extend(value);
        let checksum = crc32::checksum_ieee(&tmp);
        let mut current_position = f.seek(SeekFrom::Current(0))?;

        if saving_index == true {
            current_position = f.seek(SeekFrom::Start(0))?;
            f.seek(SeekFrom::Start(0))?;
        } else {
            let next_byte = SeekFrom::End(0);
            f.seek(next_byte)?;
        }
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(value_len as u32)?;
        f.write_all(&tmp)?;

        self.index.insert(Vec::from(key.as_ref()), current_position);
        Ok(())
    }
    fn get_at(&mut self, index: u64, get_index: bool) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.file_);
        if get_index == true {
            f = BufReader::new(&mut self.index_);
        }
        f.seek(SeekFrom::Start(index))?;
        let key_value = ActionKV::process_records(&mut f)?;
        Ok(key_value)
    }
    #[timed]
    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.index_);
        loop {
            let result_key_value = ActionKV::process_records(&mut f);
            let key_value = match result_key_value {
                Ok(key_value) => key_value,
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };
            let index_decoded = bincode::deserialize(&key_value.value);
            self.index = index_decoded.unwrap();
        }
        Ok(())
    }
    #[timed]
    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert_(key, value, false)?;
        self.store_index_on_disk(INDEX_KEY)?;
        Ok(())
    }
    #[timed]
    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let maybe_index = self.index.get(INDEX_KEY);
        if let Some(index) = maybe_index {
            let key_value = self.get_at(*index, true)?;
            let index_decoded = bincode::deserialize(&key_value.value);
            self.index = index_decoded.unwrap();
        }
        match self.index.get(key) {
            Some(&i) => {
                let kv = self.get_at(i, false).unwrap();
                return Ok(Some(kv.value));
            }
            None => return Ok(None),
        }
    }
    #[timed]
    pub fn find(&mut self, key: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
        let mut f = BufReader::new(&mut self.file_);
        let mut found_key_value: Option<(u64, ByteString)> = None;
        let mut position = f.seek(SeekFrom::Start(0))?;
        loop {
            let maybe_key_value = ActionKV::process_records(&mut f);
            let key_value = match maybe_key_value {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };
            if key == key_value.key {
                found_key_value = Some((position, key_value.value));
            }
            position = f.seek(SeekFrom::Current(0))?;
        }
        Ok(found_key_value)
    }
    #[timed]
    #[inline(always)]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        let result = self.insert(key, b"");
        self.index.remove(key);
        result
    }
    #[timed]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;
    use serial_test::serial;
    use std::fs::{remove_dir, remove_file};

    struct TestCtx {
        test_file: ActionKV,
    }
    impl TestCtx {
        fn setup() -> Self {
            Self {
                test_file: ActionKV::open(Path::new("test_foo")).expect("Unable to open file!"),
            }
        }
    }
    impl Drop for TestCtx {
        fn drop(&mut self) {
            if Path::new("test_foo").exists() {
                remove_file(Path::new("test_foo/data")).expect("failed to del file");
                remove_file(Path::new("test_foo/index")).expect("failed to del file");
                remove_dir("test_foo").expect("failed to del folder");
            }
        }
    }
    #[fixture]
    fn ctx() -> TestCtx {
        TestCtx::setup()
    }
    #[rstest]
    #[serial]
    fn test_load(mut ctx: TestCtx) {
        ctx.test_file.load().unwrap();
        assert_eq!(ctx.test_file.index.len(), 0);
        let key = b"foo";
        let value = b"bar";
        for i in 0..9 {
            let key = format!("{:?}{}", key, i);
            let new_key = key.as_bytes();
            ctx.test_file
                .insert(new_key, value)
                .expect("Unable to insert key value pair into ActionKV file!");
        }
        //index
        assert_eq!(ctx.test_file.index.len(), 1);
    }
    #[rstest]
    #[serial]
    fn test_insert_and_get(mut ctx: TestCtx) {
        let key = b"foo";
        let value = b"bar";
        ctx.test_file
            .insert(key, value)
            .expect("Unable to insert key value pair into ActionKV file!");
        let get_value = ctx
            .test_file
            .get(b"foo")
            .expect("Unable to get value pair")
            .expect("Didnt find value under that key");
        let decode_value =
            String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("bar", decode_value);
    }

    #[rstest]
    #[serial]
    fn test_get_at(mut ctx: TestCtx) {
        let key = b"foo";
        let value = b"bar";
        ctx.test_file
            .insert(key, value)
            .expect("Unable to insert key value pair into ActionKV file!");
        let get_value = ctx
            .test_file
            .get_at(0, false)
            .expect("Unable to get value pair");
        let decode_value =
            String::from_utf8(get_value.value).expect("unable to decode the value into string");
        let decode_key =
            String::from_utf8(get_value.key).expect("unable to decode the value into string");
        assert_eq!("foo", decode_key);
        assert_eq!("bar", decode_value);
    }
    #[rstest]
    #[serial]
    fn test_find(mut ctx: TestCtx) {
        let key = b"foo";
        let value = b"bar";
        let mut test_file = ActionKV::open(Path::new("test_foo")).expect("Unable to open file!");
        ctx.test_file
            .insert(key, value)
            .expect("Unable to insert key value pair into ActionKV file!");
        let find_value = test_file
            .find(key)
            .expect("Unable to get value pair")
            .unwrap();
        let decode_key =
            String::from_utf8(find_value.1).expect("unable to decode the value into string");
        assert_eq!("bar", decode_key);
        assert_eq!(find_value.0, 0);
    }
    #[rstest]
    #[serial]
    fn test_delete(mut ctx: TestCtx) {
        let key = b"foo";
        let value = b"bar";
        ctx.test_file
            .insert(key, value)
            .expect("Unable to insert key value pair into ActionKV file!");
        let get_value = ctx
            .test_file
            .get(b"foo")
            .expect("Unable to get value pair")
            .expect("Didnt find value under that key");
        let decode_value =
            String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("bar", decode_value);
        ctx.test_file
            .delete(key)
            .expect("unable to delete value at key");
        let get_value = ctx.test_file.get(b"foo").expect("Unable to get value pair");
        if None == get_value {
            assert!(true);
        }
    }
    #[rstest]
    #[serial]
    fn test_update(mut ctx: TestCtx) {
        let key = b"foo";
        let value = b"bar";
        ctx.test_file
            .insert(key, value)
            .expect("Unable to insert key value pair into ActionKV file!");
        let get_value = ctx
            .test_file
            .get(b"foo")
            .expect("Unable to get value pair")
            .expect("Didnt find value under that key");
        let decode_value =
            String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("bar", decode_value);
        ctx.test_file
            .update(key, b"foo")
            .expect("Unable to update value at the key");
        let get_value = ctx
            .test_file
            .get(b"foo")
            .expect("Unable to get value pair")
            .expect("Didnt find value under that key");
        let decode_value =
            String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("foo", decode_value);
    }
}
