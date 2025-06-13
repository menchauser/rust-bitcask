use std::collections::HashMap;
use std::env::current_dir;
use std::fs::{create_dir, create_dir_all, exists, File};
use std::io;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

struct Datastore {
    db_dir: PathBuf,
    active_file: File,
    keydir: HashMap<String, String>,
}

struct KeydirEntry {
    file: PathBuf,
    value_sz: u32,
    value_pos: u32,
    timestamp: u64,
}

enum DatastoreError {
    NonExistentDatastore,
    NonDirectoryDatastore,
    IoError { cause: io::Error },
}

impl From<io::Error> for DatastoreError {
    fn from(value: io::Error) -> Self {
        DatastoreError::IoError { cause: value }
    }
}

fn load_keydir(db_dir: PathBuf) -> Result<HashMap<String, KeydirEntry>, DatastoreError> {
    // check that db_dir exists and is directory
    let db_dir_exists = db_dir.try_exists()?;
    if !db_dir_exists {
        return Result::Err(DatastoreError::NonExistentDatastore);
    }
    if !db_dir.is_dir() {
        return Result::Err(DatastoreError::NonDirectoryDatastore);
    }

    // Load list of paths inside directory
    let result: Vec<PathBuf> = db_dir
        .read_dir()?
        .filter_map(|file| {
            let path = file.map(|f| f.path());
            path.ok()
                .filter(|p| p.is_file() && p.extension().filter(|e| *e == "dat").is_some_and(|x| x))
        })
        .collect();

    for db_file in db_dir.read_dir()? {
        let db_file = db_file?;
        let path = db_file.path();
        if path.is_file() && path.ends_with(".dat") {
            // This is data file, so we import it in keydir
        }
    }

    // Traverse them

    Result::Ok(HashMap::new())
}

impl Datastore {
    // Accept path to directory and init DB
    fn new(db_dir: PathBuf) -> Result<Self, DatastoreError> {
        eprintln!("Initialize datastore on path {}", &db_dir.display());
        // New data file name
        let epoch = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let new_file_name = std::format!("data{}.dat", epoch);
        eprintln!("Try new active file: {}...", new_file_name);
        let new_path = db_dir.join(new_file_name);
        if exists(&new_path).unwrap() {
            panic!(
                "Cannot create new datastore file {}: already exists",
                &new_path.display()
            )
        }
        let mut active_file = File::create(new_path).unwrap();

        // TODO: read existing data to keydir
        let mut keydir: HashMap<String, String> = HashMap::new();

        let reader = BufReader::new(&mut active_file);
        // now let's try reading some
        // TODO: implement reading/writing u32/u64
        // read header: CRC32: u32, timestamp: u64, ksize: u32, vsize: u32 = 20 bytes
        let mut header = [0u8; 20];
        let len = reader.read(&mut header)?;
        if len == 20 {
            // all good
        }

        Result::Ok(Self {
            db_dir: db_dir,
            active_file: active_file,
            keydir: keydir,
        })
    }

    fn get(&self, k: &String) -> Option<&String> {
        // TODO
        Option::None
    }

    fn insert(&mut self, k: &String, v: &String) -> Option<&String> {
        //TODO
        Option::None
    }
}

impl Drop for Datastore {
    fn drop(&mut self) {
        eprintln!("Closing database...");
    }
}

fn main() {
    // convert u64 to byte array and back
    let now = SystemTime::now();

    let epoch_ts = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let bytes = epoch_ts.to_be_bytes();

    println!("Number: {:x?}", epoch_ts);
    println!("Bytes: {:02x?}", bytes);

    let restored = u64::from_be_bytes(bytes);
    println!("Restored: {:x?}", restored);

    let db_dir = current_dir().unwrap().join("testdb");
    create_dir_all(&db_dir).unwrap();

    let datastore = Datastore::new(db_dir);
}
