use super::{Env, FileLock, RandomAccessFile, SequentialFile, WritableFile};
use crate::util::{DBError, Result};
use std::{ffi::OsString, fs, io, path::Path};

pub struct PosixEnv {}

impl Env for PosixEnv {
    /// The returned file will only be accessed by one thread at a time.
    fn new_sequential_file(&self, fname: &str) -> Result<Box<dyn SequentialFile>> {
        todo!()
    }

    /// The returned file may be concurrently accessed by multiple threads.
    fn new_random_access_file(&self, fname: &str) -> Result<Box<dyn RandomAccessFile>> {
        todo!()
    }

    /// The returned file will only be accessed by one thread at a time.
    fn new_writable_file(&self, fname: &str) -> Result<Box<dyn WritableFile>> {
        todo!()
    }

    fn new_appendable_file(&self, fname: &str) -> Result<Box<dyn WritableFile>> {
        todo!()
    }

    fn file_exists(&self, fname: &str) -> bool {
        Path::new(fname).exists()
    }

    fn get_children(&self, dname: &str) -> Result<Vec<OsString>> {
        match fs::read_dir(dname) {
            Ok(entries) => Ok(entries
                .filter_map(|entry| entry.map(|e| e.file_name()).ok())
                .collect()),
            Err(error) => Err(to_db_error(dname, error)),
        }
    }

    fn remove_file(&self, fname: &str) -> Result<()> {
        match fs::remove_file(fname) {
            Ok(()) => Ok(()),
            Err(error) => Err(to_db_error(fname, error)),
        }
    }

    fn create_dir(&self, dname: &str) -> Result<()> {
        match fs::create_dir(dname) {
            Ok(()) => Ok(()),
            Err(error) => Err(to_db_error(dname, error)),
        }
    }

    fn remove_dir(&self, dname: &str) -> Result<()> {
        match fs::remove_dir(dname) {
            Ok(()) => Ok(()),
            Err(error) => Err(to_db_error(dname, error)),
        }
    }

    fn get_file_size(&self, fname: &str) -> Result<u64> {
        match fs::metadata(fname) {
            Ok(data) => Ok(data.len()),
            Err(error) => Err(to_db_error(fname, error)),
        }
    }

    fn rename_file(&self, src: &str, target: &str) -> Result<()> {
        match fs::rename(src, target) {
            Ok(()) => Ok(()),
            Err(error) => Err(to_db_error(src, error)),
        }
    }

    fn lock_file(&self, fname: &str) -> Result<Box<dyn FileLock>> {
        todo!()
    }
    fn unlock_file(&self, lock: Box<dyn FileLock>) -> Result<()> {
        todo!()
    }
}

fn to_db_error(target: &str, error: io::Error) -> DBError {
    let msg = format!("{}: {}", target, &error.to_string());
    match error.kind() {
        io::ErrorKind::NotFound => DBError::not_found(&msg),
        _ => DBError::io_error(&msg),
    }
}

struct PosixWritableFile {}

impl WritableFile for PosixWritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        todo!()
    }

    fn close(&mut self) -> Result<()> {
        todo!()
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }

    fn sync(&mut self) -> Result<()> {
        todo!()
    }
}
