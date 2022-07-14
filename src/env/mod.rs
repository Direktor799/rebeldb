use crate::util::Result;
use std::{ffi::OsString, sync::Arc};

mod posix_env;

/// An Env is an interface used by the leveldb implementation to access
/// operating system functionality like the filesystem etc.  Callers
/// may wish to provide a custom Env object when opening a database to
/// get fine gain control; e.g., to rate limit file system operations.
///
/// All Env implementations are safe for concurrent access from
/// multiple threads without any external synchronization.
pub trait Env {
    /// The returned file will only be accessed by one thread at a time.
    fn new_sequential_file(&self, fname: &str) -> Result<Box<dyn SequentialFile>>;

    /// The returned file may be concurrently accessed by multiple threads.
    fn new_random_access_file(&self, fname: &str) -> Result<Box<dyn RandomAccessFile>>;

    /// The returned file will only be accessed by one thread at a time.
    fn new_writable_file(&self, fname: &str) -> Result<Box<dyn WritableFile>>;

    fn new_appendable_file(&self, fname: &str) -> Result<Box<dyn WritableFile>>;
    fn file_exists(&self, fname: &str) -> bool;
    fn get_children(&self, dname: &str) -> Result<Vec<OsString>>;
    fn remove_file(&self, fname: &str) -> Result<()>;
    fn create_dir(&self, dname: &str) -> Result<()>;
    fn remove_dir(&self, dname: &str) -> Result<()>;
    fn get_file_size(&self, fname: &str) -> Result<u64>;
    fn rename_file(&self, src: &str, target: &str) -> Result<()>;
    fn lock_file(&self, fname: &str) -> Result<Box<dyn FileLock>>;
    fn unlock_file(&self, lock: Box<dyn FileLock>) -> Result<()>;
    fn new_logger(&self, fname: &str) -> Result<Box<dyn Logger + '_>>;
    // todo: more
    // fn schedule(function: Box<dyn FnMut()>);
}

/// A file abstraction for reading sequentially through a file
pub trait SequentialFile {
    fn read(&mut self, dst: &mut [u8]) -> Result<()>;
    fn skip(&mut self, n: usize) -> Result<()>;
}

/// A file abstraction for randomly reading the contents of a file.
pub trait RandomAccessFile {
    fn read(&self, offset: usize, dst: &mut [u8]) -> Result<()>;
}

/// A file abstraction for sequential writing.  The implementation
/// must provide buffering since callers may append small fragments
/// at a time to the file.
pub trait WritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

pub trait FileLock {}

pub trait Logger {
    // todo
    fn log(&self, info: &str);
}

fn write_data_to_file_inner(
    env: Arc<dyn Env>,
    data: &[u8],
    fname: &str,
    should_sync: bool,
) -> Result<()> {
    let mut file = env.new_writable_file(fname)?;
    let mut result = file.append(data);
    if result.is_ok() && should_sync {
        result = file.sync();
    }
    if result.is_ok() {
        result = file.close();
    }
    drop(file);
    if result.is_err() {
        let _ = env.remove_file(fname);
    }
    result
}

pub fn write_data_to_file(env: Arc<dyn Env>, data: &[u8], fname: &str) -> Result<()> {
    write_data_to_file_inner(env, data, fname, false)
}

pub fn write_data_to_file_sync(env: Arc<dyn Env>, data: &[u8], fname: &str) -> Result<()> {
    write_data_to_file_inner(env, data, fname, true)
}
