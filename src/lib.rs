#[macro_use]
extern crate serde_derive;

extern crate byteorder;
extern crate crc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, remove_file},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};
use std::panic;

pub type ByteString = Vec<u8>;
pub type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct ActionKV {
    file_: File,
    pub index: HashMap<ByteString, u64>,
}

/*
    THIS IS BITCASK FILE FORMAT
    checksum | key_len | value_len |     key      |     value
    [u32;1]    [u32;1]   [u32;1]     [u8;key_len]   [u8;value_len]
*/
impl ActionKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file_ = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();
        Ok(ActionKV { file_, index })
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
    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.file_);
        loop {
            let current_pos = f.seek(SeekFrom::Current(0))?;
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
            self.index.insert(key_value.key, current_pos);
        }
        Ok(())
    }
    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let mut f = BufWriter::new(&mut self.file_);

        let key_len = key.as_ref().len();
        let value_len = value.as_ref().len();
        let mut tmp = ByteString::with_capacity(key_len + value_len);
        tmp.extend(key);
        tmp.extend(value);
        let checksum = crc32::checksum_ieee(&tmp);

        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;

        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(value_len as u32)?;
        f.write_all(&tmp)?;

        self.index.insert(Vec::from(key.as_ref()), current_position);
        Ok(())
    }
    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            Some(position) => *position,
            None => return Ok(None),
        };
        let key_value = self.get_at(position)?;
        Ok(Some(key_value.value))
    }
    pub fn get_at(&mut self, index: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.file_);
        f.seek(SeekFrom::Start(index))?;
        let key_value = ActionKV::process_records(&mut f)?;
        Ok(key_value)
    }
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
    #[inline(always)]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        let result = self.insert(key, b"");
        self.index.remove(key);
        result
    }

    #[inline(always)]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let key = b"foo";
        let value = b"bar";
        let mut test_file = ActionKV::open(Path::new("test_foo")).expect("Unable to open file!");
        test_file.load().expect("Unable to load values of the ActionKV file!");
        test_file.insert(key, value).expect("Unable to insert key value pair into ActionKV file!");
        let get_value = test_file.get(b"foo").expect("Unable to get value pair").expect("Didnt find value under that key");
        let decode_value = String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("bar", decode_value);
        if Path::new("test_foo").exists() {
            remove_file(Path::new("test_foo")).expect("failed to del file");
        }
    }
    #[test]
    fn test_get_at() {
        let key = b"foo";
        let value = b"bar";
        let mut test_file = ActionKV::open(Path::new("test_foo")).expect("Unable to open file!");
        test_file.load().expect("Unable to load values of the ActionKV file!");
        test_file.insert(key, value).expect("Unable to insert key value pair into ActionKV file!");
        let get_value = test_file.get_at(0).expect("Unable to get value pair");
        let decode_value = String::from_utf8(get_value.value).expect("unable to decode the value into string");
        let decode_key = String::from_utf8(get_value.key).expect("unable to decode the value into string");
        assert_eq!("foo", decode_key);
        assert_eq!("bar", decode_value);
        if Path::new("test_foo").exists() {
            remove_file(Path::new("test_foo")).expect("failed to del file");
        }
    }
    #[test]
    fn test_find() {
        let key = b"foo";
        let value = b"bar";
        let mut test_file = ActionKV::open(Path::new("test_foo")).expect("Unable to open file!");
        test_file.load().expect("Unable to load values of the ActionKV file!");
        test_file.insert(key, value).expect("Unable to insert key value pair into ActionKV file!");
        test_file.insert(key, value).expect("Unable to insert key value pair into ActionKV file!");
        test_file.insert(b"bar", b"foo").expect("Unable to insert key value pair into ActionKV file!");
        let find_value = test_file.find(b"bar").expect("Unable to get value pair").unwrap();
        let decode_key = String::from_utf8(find_value.1).expect("unable to decode the value into string");
        assert_eq!("foo", decode_key);
        assert_eq!(find_value.0, 36);
        if Path::new("test_foo").exists() {
            remove_file(Path::new("test_foo")).expect("failed to del file");
        }
    }
    #[test]
    fn test_delete() {
        let key = b"foo";
        let value = b"bar";
        let mut test_file = ActionKV::open(Path::new("test_foo")).expect("Unable to open file!");
        test_file.load().expect("Unable to load values of the ActionKV file!");
        test_file.insert(key, value).expect("Unable to insert key value pair into ActionKV file!");
        let get_value = test_file.get(b"foo").expect("Unable to get value pair").expect("Didnt find value under that key");
        let decode_value = String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("bar", decode_value);
        test_file.delete(key).expect("unable to delete value at key");
        let get_value = test_file.get(b"foo").expect("Unable to get value pair");
        if None == get_value {
            assert!(true);
        }
        if Path::new("test_foo").exists() {
            remove_file(Path::new("test_foo")).expect("failed to del file");
        }
    }
    #[test]
    fn test_update() {
        let key = b"foo";
        let value = b"bar";
        let mut test_file = ActionKV::open(Path::new("test_foo")).expect("Unable to open file!");
        test_file.load().expect("Unable to load values of the ActionKV file!");
        test_file.insert(key, value).expect("Unable to insert key value pair into ActionKV file!");
        let get_value = test_file.get(b"foo").expect("Unable to get value pair").expect("Didnt find value under that key");
        let decode_value = String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("bar", decode_value);
        test_file.update(key, b"foo").expect("Unable to update value at the key");
        let get_value = test_file.get(b"foo").expect("Unable to get value pair").expect("Didnt find value under that key");
        let decode_value = String::from_utf8(get_value).expect("unable to decode the value into string");
        assert_eq!("foo", decode_value);
        if Path::new("test_foo").exists() {
            remove_file(Path::new("test_foo")).expect("failed to del file");
        }
    }
}
