use std::{
    cell::RefCell,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    mem::MaybeUninit,
    path::Path,
    ptr::null_mut,
    thread::{self, ThreadId},
    time,
};

use chrono::Local;

use super::{Env, FileLock, Logger, RandomAccessFile, SequentialFile, WritableFile};
use crate::util::{Error, Result};

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

    fn new_logger(&self, fname: &str) -> Result<Box<dyn Logger + '_>> {
        match OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(fname)
        {
            Ok(file) => Ok(Box::new(PosixLogger::new(file))),
            Err(error) => return Err(to_db_error(fname, error)),
        }
    }
}

fn to_db_error(target: &str, error: io::Error) -> Error {
    let msg = format!("{}: {}", target, &error.to_string());
    match error.kind() {
        io::ErrorKind::NotFound => Error::not_found(&msg),
        _ => Error::io_error(&msg),
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

struct PosixLogger {
    file: RefCell<File>,
}

impl PosixLogger {
    fn new(file: File) -> Self {
        Self {
            file: RefCell::new(file),
        }
    }
}

impl Logger for PosixLogger {
    fn log(&self, info: &str) {
        // Record the time as close to the Logv() call as possible.
        let time = Local::now().format("%Y/%m/%d-%H:%M:%S%.6f").to_string();
        // Record the thread ID.
        let thread_id = thread::current().id();
        let mut info = format!("{} {:?} {}", time, thread_id, info);
        if info.chars().last().unwrap() != '\n' {
            info += "\n";
        }
        let mut file_inner = self.file.borrow_mut();
        file_inner.write(info.as_bytes()).unwrap();
        file_inner.flush().unwrap();
    }
}
