use chrono::{NaiveDate, NaiveDateTime};
use polars::export::regex::Regex;
use std::collections::HashSet;
use std::fs::{self, DirEntry, File};
use std::path::PathBuf;
use uuid::Uuid;

use crate::schema::OutputType;

pub fn list_dirs(path: &PathBuf) -> Vec<fs::DirEntry> {
    match fs::read_dir(path) {
        Ok(paths) => paths
            .filter(|x| x.is_ok() && x.as_ref().unwrap().metadata().unwrap().is_dir())
            .map(|x| x.unwrap())
            .collect(),
        Err(why) => {
            println!("{}: {:?}", why, path);
            vec![]
        }
    }
}

pub fn list_files(path: PathBuf) -> Vec<fs::DirEntry> {
    match fs::read_dir(path) {
        Ok(paths) => paths
            .filter(|x| x.is_ok() && x.as_ref().unwrap().metadata().unwrap().is_file())
            .map(|x| x.unwrap())
            .collect(),
        Err(_) => vec![],
    }
}

fn traverse_dirs(path: &PathBuf) -> Vec<fs::DirEntry> {
    let mut dirs = list_dirs(path);
    dirs.append(
        &mut dirs
            .iter()
            .map(|x| traverse_dirs(&x.path().to_str().expect("not a valid dir name").into()))
            .flatten()
            .collect(),
    );
    return dirs;
}

pub fn find_uuid_folders(path: &PathBuf, uuid: &str) -> HashSet<PathBuf> {
    HashSet::from_iter(
        traverse_dirs(path)
            .into_iter()
            .filter(|x| x.file_name().to_str().unwrap_or("").contains(uuid))
            .map(|x| x.path()),
    )
}

pub fn find_uuids(path: &PathBuf) -> HashSet<String> {
    HashSet::from_iter(list_dirs(path).into_iter().map(|x| {
        x.path()
            .to_string()
            .to_owned()
            .splitn(10, "_")
            .last()
            .unwrap()
            .to_string()
    }))
}

pub fn find_inital_app_start(folders: &HashSet<PathBuf>) -> Option<NaiveDate> {
    folders
        .into_iter()
        .map(|x| {
            NaiveDate::parse_from_str(
                x.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
                    .splitn(10, "/")
                    .last()
                    .unwrap_or("0")
                    .split_once("_")
                    .into_iter()
                    .next()
                    .unwrap_or(("0", "0"))
                    .0,
                "%Y-%m-%d",
            )
            .unwrap_or(NaiveDate::default())
        })
        .reduce(|a, b| if a < b { a } else { b })
}

pub fn find_uuids_after(path: &PathBuf, date: &NaiveDate) -> HashSet<String> {
    HashSet::from_iter(
        list_dirs(path)
            .into_iter()
            .map(|x| {
                x.path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
                    .splitn(10, "_")
                    .into_iter()
                    .map(|x| x.to_owned())
                    .collect::<Vec<String>>()
            })
            .filter(|split| match split.first() {
                Some(first) => match NaiveDate::parse_from_str(first.as_str(), "%Y-%m-%d") {
                    Ok(parsed_date) => &parsed_date >= date,
                    Err(_) => {
                        println!("could not parse date {}", first);
                        false
                    }
                },
                None => false,
            })
            .map(|splits| splits.last().unwrap().to_string()),
    )
}

pub fn find_sensors(user_folders: &HashSet<PathBuf>) -> HashSet<String> {
    find_sensor_names(get_subfolders(user_folders, OutputType::logs))
}

fn get_subfolders(user_folders: &HashSet<PathBuf>, subdir_type: OutputType) -> Vec<DirEntry> {
    user_folders
        .into_iter()
        .map(|dir_entry| {
            let mut path = dir_entry.clone();
            path.push(subdir_type.subdir());
            list_files(path)
        })
        .flatten()
        .collect()
}

pub fn find_first_activity(user_folders: &HashSet<PathBuf>) -> Option<NaiveDateTime> {
    NaiveDateTime::from_timestamp_millis(
        get_subfolders(user_folders, OutputType::logs)
            .into_iter()
            .map(|x| path_to_begin_timestamp(&x).parse::<i64>().unwrap_or(0))
            .reduce(|a, b| a.max(b))
            .unwrap_or(0),
    )
}

fn find_sensor_names(files: Vec<DirEntry>) -> HashSet<String> {
    let sensor_regex = Regex::new(r"FT[(A-F|0-9)]{3}").unwrap();
    let mut set: HashSet<String> = HashSet::new();
    for entry in files.into_iter() {
        if let Ok(content) = fs::read_to_string(entry.path()) {
            if let Some(matches) = sensor_regex.captures(content.as_str()) {
                if let Some(first) = matches.get(0) {
                    set.insert(first.as_str().to_string());
                    return set;
                }
            }
        }
    }
    set
}

pub fn concat_csv_files(paths: Vec<DirEntry>) -> PathBuf {
    let mut temp_dir = std::env::temp_dir();
    let uuid = Uuid::new_v4().to_string();
    temp_dir.push(uuid);
    File::create(temp_dir.clone()).expect("could not create file");
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(temp_dir.clone())
        .expect("could not open temp file");
    for path in paths {
        let mut f2 = fs::OpenOptions::new().read(true).open(path.path()).unwrap();
        match std::io::copy(&mut f2, &mut file) {
            Ok(_) => {}
            Err(_) => {}
        }
    }
    temp_dir
}

fn path_to_begin_timestamp(f: &DirEntry) -> String {
    let i: String = f
        .path()
        .file_name()
        .expect("no filename found")
        .to_str()
        .unwrap()
        .to_string();
    match i.split_once("-") {
        Some(p) => p.0.into(),
        None => {
            println!("could not parse file name: {}", i);
            "0".into()
        }
    }
}

trait ToString {
    fn to_string(self) -> String;
}

impl ToString for DirEntry {
    fn to_string(self) -> String {
        self.path().to_string()
    }
}

impl ToString for PathBuf {
    fn to_string(self) -> String {
        self.to_str().expect("could not convert to string").into()
    }
}

trait SinceEpoch {
    fn ms_since_epoch(&self) -> i64;
}

impl SinceEpoch for NaiveDate {
    fn ms_since_epoch(&self) -> i64 {
        self.signed_duration_since(NaiveDate::default())
            .num_milliseconds()
    }
}

pub fn filter_files_by_date(files: Vec<DirEntry>, date: NaiveDate) -> Vec<DirEntry> {
    let begin = date.ms_since_epoch();
    let end = date.succ_opt().unwrap().ms_since_epoch();
    files
        .into_iter()
        .filter(|x| {
            let b = path_to_begin_timestamp(x).parse::<i64>().unwrap_or(0);
            begin <= b && b <= end
        })
        .collect()
}
