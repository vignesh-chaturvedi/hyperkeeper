//! Append-only JSONL logs that start a new file each UTC day, named `<prefix>-YYYYMMDD.jsonl`.
//! A long-running service then leaves each finished day closed, so it can be compressed while
//! the service keeps running. A restart on the same day appends to that day's file.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use chrono::{NaiveDate, Utc};
use serde::Serialize;

pub struct DailyLog {
    dir: PathBuf,
    prefix: String,
    day: NaiveDate,
    file: File,
}

impl DailyLog {
    /// Opens today's file, so a directory that can't be written fails at startup.
    pub fn open(dir: &Path, prefix: impl Into<String>) -> anyhow::Result<Self> {
        let dir = dir.to_path_buf();
        let prefix = prefix.into();
        let day = Utc::now().date_naive();
        let file = append(&file_path(&dir, &prefix, day))?;
        Ok(Self {
            dir,
            prefix,
            day,
            file,
        })
    }

    /// The file rows are going to now.
    pub fn path(&self) -> PathBuf {
        file_path(&self.dir, &self.prefix, self.day)
    }

    /// Appends `row` as one line, moving to a new file first if the UTC date has changed.
    pub fn write(&mut self, row: &impl Serialize) -> anyhow::Result<()> {
        self.write_on(Utc::now().date_naive(), row)
    }

    fn write_on(&mut self, today: NaiveDate, row: &impl Serialize) -> anyhow::Result<()> {
        // One write per row, so a row never straddles two files.
        let mut line = serde_json::to_vec(row)?;
        line.push(b'\n');
        if today != self.day {
            self.file = append(&file_path(&self.dir, &self.prefix, today))?;
            self.day = today;
        }
        self.file.write_all(&line)?;
        Ok(())
    }
}

fn file_path(dir: &Path, prefix: &str, day: NaiveDate) -> PathBuf {
    dir.join(format!("{prefix}-{}.jsonl", day.format("%Y%m%d")))
}

fn append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_a_new_file_each_day() {
        let dir =
            std::env::temp_dir().join(format!("hyperkeeper-daily-log-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut log = DailyLog::open(&dir, "shadow-test").unwrap();
        let today = log.day;
        let tomorrow = today.succ_opt().unwrap();

        log.write_on(today, &serde_json::json!({ "n": 1 })).unwrap();
        log.write_on(today, &serde_json::json!({ "n": 2 })).unwrap();
        log.write_on(tomorrow, &serde_json::json!({ "n": 3 }))
            .unwrap();

        let read = |day| std::fs::read_to_string(file_path(&dir, "shadow-test", day)).unwrap();
        assert_eq!(read(today), "{\"n\":1}\n{\"n\":2}\n");
        assert_eq!(read(tomorrow), "{\"n\":3}\n");
        assert_eq!(log.path(), file_path(&dir, "shadow-test", tomorrow));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
