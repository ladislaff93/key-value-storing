use libactionkv::{ActionKV, ByteStr, ByteString};
use log::{info, log_enabled, Level};
use std::path::Path;

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage:
    akv_mem.exe FILE get KEY
    akv_mem.exe FILE delete KEY
    akv_mem.exe FILE insert KEY VALUE
    akv_mem.exe FILE update KEY VALUE
";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let f_name = args.get(1).expect(&USAGE);
    let op = args.get(2).expect(&USAGE).as_ref();
    let key: &ByteStr = args.get(3).expect(&USAGE).as_ref();
    let value_option = args.get(4);

    let mut s = ActionKV::open(Path::new(&f_name)).expect("Unable to open file");
    s.load().expect("Unable to load data from file.");
    match op {
        "get" => match s.get(key).unwrap() {
            Some(value) => {
                println!("{:?}", String::from_utf8(value).unwrap())
            }
            None => {
                println!("{:?} not found", String::from_utf8(Vec::from(key)).unwrap())
            }
        },
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
