use libactionkv::{ActionKV, ByteStr, ByteString};
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
    akv_mem.exe FILE get KEY
    akv_mem.exe FILE delete KEY
    akv_mem.exe FILE insert KEY VALUE
    akv_mem.exe FILE update KEY VALUE
";

fn store_index_on_disk(action_kv: &mut ActionKV, index_key: &ByteStr) {
    action_kv.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&action_kv.index).unwrap();
    action_kv.index = std::collections::HashMap::new();
    action_kv.insert(index_key, &index_as_bytes).unwrap();
}

fn main() {
    const INDEX_KEY: &ByteStr = b"+index";
    let args: Vec<String> = std::env::args().collect();
    let f_name = args.get(1).expect(&USAGE);
    let op = args.get(2).expect(&USAGE).as_ref();
    let key: &ByteStr = args.get(3).expect(&USAGE).as_ref();
    let value_option = args.get(4);

    let mut s = ActionKV::open(Path::new(&f_name)).expect("Unable to open file");
    s.load().expect("Unable to load data from file.");

    match op {
        "get" => {
            let index_as_bytes = s.get(&INDEX_KEY).unwrap().unwrap();
            let index_decoded = bincode::deserialize(&index_as_bytes);
            let index: HashMap<ByteString, u64> = index_decoded.unwrap();
            match index.get(key) {
                Some(&i) => {
                    let kv = s.get_at(i).unwrap();
                    println!("{:?}", String::from_utf8(kv.value).unwrap())
                }
                None => {
                    println!("{:?} not found", String::from_utf8(Vec::from(key)).unwrap())
                }
            }
        }
        "delete" => match s.delete(&key) {
            Ok(_) => {
                println!(
                    "Value under {:?} was deleted",
                    String::from_utf8(Vec::from(key)).unwrap()
                )
            }
            Err(_) => {
                println!(
                    "{:?} not found in file",
                    String::from_utf8(Vec::from(key)).unwrap()
                )
            }
        },
        "insert" => {
            let value = value_option.expect(&USAGE).as_ref();
            match s.insert(&key, &value) {
                Ok(_) => {
                    println!(
                        "{:?} was inserted under {:?}",
                        String::from_utf8(Vec::from(value)).unwrap(),
                        String::from_utf8(Vec::from(key)).unwrap()
                    );
                    store_index_on_disk(&mut s, INDEX_KEY);
                }
                Err(_) => {
                    println!(
                        "{:?} was not inserted under {:?}. {:?}",
                        String::from_utf8(Vec::from(value)).unwrap(),
                        String::from_utf8(Vec::from(key)).unwrap(),
                        &USAGE
                    )
                }
            }
        }
        "update" => {
            let value = value_option.expect(&USAGE).as_ref();
            match s.update(&key, &value) {
                Ok(_) => {
                    println!(
                        "{:?} was updated under {:?}",
                        String::from_utf8(Vec::from(value)).unwrap(),
                        String::from_utf8(Vec::from(key)).unwrap()
                    );
                    store_index_on_disk(&mut s, INDEX_KEY);
                }
                Err(_) => {
                    println!(
                        "{:?} was not updated under {:?}. {:?}",
                        String::from_utf8(Vec::from(value)).unwrap(),
                        String::from_utf8(Vec::from(key)).unwrap(),
                        &USAGE
                    )
                }
            }
        }
        _ => {
            eprintln!("{:?}", &USAGE)
        }
    }
}
