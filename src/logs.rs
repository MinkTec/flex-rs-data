use crate::{fs::get_subdirs, misc::parse_dart_timestring, schema::OutputType};
use chrono::NaiveDateTime;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf, str::FromStr};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: NaiveDateTime,
    pub logger: String,
    pub log_level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    OFF,
    SHOUT,
    SEVERE,
    WARNING,
    INFO,
    CONFIG,
    FINE,
    FINER,
    FINEST,
}

impl FromStr for LogLevel {
    type Err = ParseLogEntryError;

    fn from_str(s: &str) -> Result<Self, ParseLogEntryError> {
        Ok(match s.trim() {
            "OFF" => LogLevel::OFF,
            "SHOUT" => LogLevel::SHOUT,
            "SEVER" => LogLevel::SEVERE,
            "WARNING" => LogLevel::WARNING,
            "INFO" => LogLevel::INFO,
            "CONFIG" => LogLevel::CONFIG,
            "FINE" => LogLevel::FINE,
            "FINER" => LogLevel::FINER,
            "FINEST" => LogLevel::FINEST,
            _ => return Err(ParseLogEntryError),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseLogEntryError;

impl FromStr for LogEntry {
    type Err = ParseLogEntryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let split = value.split(",").into_iter().collect::<Vec<&str>>();

        if split.len() < 4 {
            return Err(ParseLogEntryError);
        }
        let timestamp = match parse_dart_timestring(split[0]) {
            Ok(t) => t,
            Err(_) => return Err(ParseLogEntryError),
        };

        let log_level = LogLevel::from_str(split[2])?;

        Ok(LogEntry {
            timestamp,
            logger: split[1].trim().to_string(),
            log_level,
            message: split[3..].join(", ").trim().to_string(),
        })
    }
}

pub fn find_in_logs(dirs: &Vec<PathBuf>, regex: Regex) -> Vec<LogEntry> {
    let mut m: Vec<LogEntry> = vec![];
    for entry in get_subdirs(&dirs, OutputType::logs).into_iter() {
        if let Ok(content) = fs::read_to_string(entry.path()) {
            for line in content.split("\n") {
                if regex.is_match(line) {
                    if let Ok(entry) = LogEntry::from_str(line) {
                        m.push(entry);
                    }
                }
            }
        }
    }
    m
}
