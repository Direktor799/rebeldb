use self::{
    config::{DBConfig, ReadConfig, WriteConfig},
    write_batch::WriteBatch,
};
use crate::Result;

mod config;
mod write_batch;

pub struct DB {}

impl DB {
    fn open(name: &str, config: &DBConfig) -> Result<Self> {
        todo!()
    }
    fn put(&mut self, key: &[u8], value: &[u8], config: &WriteConfig) -> Result<()> {
        todo!()
    }
    fn delete(&mut self, key: &[u8], config: &WriteConfig) -> Result<()> {
        todo!()
    }
    fn write(&mut self, updates: &WriteBatch, config: &WriteConfig) -> Result<()> {
        todo!()
    }
    fn get(&self, key: &[u8], config: &ReadConfig) -> Result<Vec<u8>> {
        todo!()
    }
    // TODO: finish it later
    // fn iter(&self, config:&ReadConfig)
}
