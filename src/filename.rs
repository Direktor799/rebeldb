use std::sync::Arc;

use crate::{
    env::{write_data_to_file_sync, Env},
    util::Result,
};

#[derive(PartialEq, Debug)]
pub enum FileType {
    LogFile,
    DBLockFile,
    TableFile,
    DescriptorFile,
    CurrentFile,
    TempFile,
    InfoLogFile,
}

pub fn log_file_name(dbname: &str, number: u64) -> String {
    assert!(number > 0);
    format!("{}/{:06}.{}", dbname, number, "log")
}

pub fn table_file_name(dbname: &str, number: u64) -> String {
    assert!(number > 0);
    format!("{}/{:06}.{}", dbname, number, "ldb")
}

pub fn sst_table_file_name(dbname: &str, number: u64) -> String {
    assert!(number > 0);
    format!("{}/{:06}.{}", dbname, number, "sst")
}

pub fn descriptor_file_name(dbname: &str, number: u64) -> String {
    assert!(number > 0);
    format!("{}/MANIFEST-{}", dbname, number)
}

pub fn current_file_name(dbname: &str) -> String {
    format!("{}/CURRENT", dbname)
}

pub fn lock_file_name(dbname: &str) -> String {
    format!("{}/LOCK", dbname)
}

pub fn temp_file_name(dbname: &str, number: u64) -> String {
    format!("{}/{:06}.{}", dbname, number, "dbtmp")
}

pub fn info_log_file_name(dbname: &str) -> String {
    format!("{}/LOG", dbname)
}

pub fn old_info_log_file_name(dbname: &str) -> String {
    format!("{}/LOG.old", dbname)
}

/// Owned filenames have the form:
///    dbname/CURRENT
///    dbname/LOCK
///    dbname/LOG
///    dbname/LOG.old
///    dbname/MANIFEST-[0-9]+
///    dbname/[0-9]+.(log|sst|ldb)
pub fn parse_file_name(filename: &str) -> Option<(u64, FileType)> {
    if filename == "CURRENT" {
        Some((0, FileType::CurrentFile))
    } else if filename == "LOCK" {
        Some((0, FileType::DBLockFile))
    } else if filename == "LOG" || filename == "LOG.old" {
        Some((0, FileType::InfoLogFile))
    } else if filename.starts_with("MANIFEST-") {
        if let Ok(num) = filename[9..].parse::<u64>() {
            Some((num, FileType::DescriptorFile))
        } else {
            None
        }
    } else {
        let index = filename
            .chars()
            .position(|ch| !ch.is_numeric())
            .unwrap_or(filename.len());
        if let Ok(num) = filename[..index].parse::<u64>() {
            let file_type = match &filename[index..] {
                ".log" => FileType::LogFile,
                ".sst" | ".ldb" => FileType::TableFile,
                ".dbtmp" => FileType::TempFile,
                _ => return None,
            };
            Some((num, file_type))
        } else {
            None
        }
    }
}

pub fn set_current_file(env: Arc<dyn Env>, dbname: &str, descriptor_number: u64) -> Result<()> {
    let manifest = descriptor_file_name(dbname, descriptor_number);
    let content = &manifest[dbname.len() + 1..];
    let tmp = temp_file_name(dbname, descriptor_number);
    if let Ok(_) =
        write_data_to_file_sync(env.clone(), (content.to_string() + "\n").as_bytes(), &tmp)
    {
        env.rename_file(&tmp, &current_file_name(dbname))
    } else {
        env.remove_file(&tmp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_name_parse() {
        let cases = [
            ("100.log", 100, FileType::LogFile),
            ("0.log", 0, FileType::LogFile),
            ("0.sst", 0, FileType::TableFile),
            ("0.ldb", 0, FileType::TableFile),
            ("CURRENT", 0, FileType::CurrentFile),
            ("LOCK", 0, FileType::DBLockFile),
            ("MANIFEST-2", 2, FileType::DescriptorFile),
            ("MANIFEST-7", 7, FileType::DescriptorFile),
            ("LOG", 0, FileType::InfoLogFile),
            ("LOG.old", 0, FileType::InfoLogFile),
            (
                "18446744073709551615.log",
                18446744073709551615u64,
                FileType::LogFile,
            ),
        ];

        for (fname, number, type_) in cases {
            assert_eq!((number, type_), parse_file_name(fname).unwrap());
        }

        let errors = [
            "",
            "foo",
            "foo-dx-100.log",
            ".log",
            "",
            "manifest",
            "CURREN",
            "CURRENTX",
            "MANIFES",
            "MANIFEST",
            "MANIFEST-",
            "XMANIFEST-3",
            "MANIFEST-3x",
            "LOC",
            "LOCKx",
            "LO",
            "LOGx",
            "18446744073709551616.log",
            "184467440737095516150.log",
            "100",
            "100.",
            "100.lop",
        ];

        for fname in errors {
            assert!(parse_file_name(fname).is_none());
        }
    }

    #[test]
    fn test_file_name_construction() {
        let fname = current_file_name(&"foo");
        assert_eq!("foo/", &fname[..4]);
        assert_eq!(
            (0, FileType::CurrentFile),
            parse_file_name(&fname[4..]).unwrap()
        );

        let fname = lock_file_name(&"foo");
        assert_eq!("foo/", &fname[..4]);
        assert_eq!(
            (0, FileType::DBLockFile),
            parse_file_name(&fname[4..]).unwrap()
        );

        let fname = log_file_name(&"foo", 192);
        assert_eq!("foo/", &fname[..4]);
        assert_eq!(
            (192, FileType::LogFile),
            parse_file_name(&fname[4..]).unwrap()
        );

        let fname = table_file_name(&"bar", 200);
        assert_eq!("bar/", &fname[..4]);
        assert_eq!(
            (200, FileType::TableFile),
            parse_file_name(&fname[4..]).unwrap()
        );

        let fname = descriptor_file_name(&"bar", 100);
        assert_eq!("bar/", &fname[..4]);
        assert_eq!(
            (100, FileType::DescriptorFile),
            parse_file_name(&fname[4..]).unwrap()
        );

        let fname = temp_file_name(&"tmp", 999);
        assert_eq!("tmp/", &fname[..4]);
        assert_eq!(
            (999, FileType::TempFile),
            parse_file_name(&fname[4..]).unwrap()
        );

        let fname = info_log_file_name(&"foo");
        assert_eq!("foo/", &fname[..4]);
        assert_eq!(
            (0, FileType::InfoLogFile),
            parse_file_name(&fname[4..]).unwrap()
        );

        let fname = old_info_log_file_name(&"foo");
        assert_eq!("foo/", &fname[..4]);
        assert_eq!(
            (0, FileType::InfoLogFile),
            parse_file_name(&fname[4..]).unwrap()
        );
    }
}
