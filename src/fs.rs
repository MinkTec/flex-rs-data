use chrono::{NaiveDate, NaiveDateTime};
use polars::export::regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, DirEntry, File};
use std::path::PathBuf;
use std::str::FromStr;
use uuid::Uuid;

use crate::schema::OutputType;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct AppVersion {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
    pub build: u8,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseAppVersionError;

impl FromStr for AppVersion {
    type Err = ParseAppVersionError;

    fn from_str(version: &str) -> Result<Self, Self::Err> {
        let mut splits = version.split(".");
        let major = splits
            .next()
            .unwrap()
            .parse()
            .map_err(|_| ParseAppVersionError)?;
        let minor = splits
            .next()
            .unwrap()
            .parse()
            .map_err(|_| ParseAppVersionError)?;
        let (patch, build) = match splits.next().unwrap().split_once("-") {
            Some(e) => Ok(e),
            None => Err(ParseAppVersionError),
        }?;
        let patch = patch.parse().map_err(|_| ParseAppVersionError)?;
        let build = build.parse().map_err(|_| ParseAppVersionError)?;

        Ok(AppVersion {
            major,
            minor,
            patch,
            build,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct PhoneModel {
    pub brand: String,
    pub model: String,
}

#[derive(Debug, PartialEq, Eq)]
struct ParsePhoneModelError;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct ParsedDir {
    pub path: PathBuf,
    pub uuid: Uuid,
    pub initial_app_start: NaiveDateTime,
    pub phone: PhoneModel,
    pub app_version: AppVersion,
}

impl ParsedDir {}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseFlexDataDirNameError;

impl FromStr for ParsedDir {
    type Err = ParseFlexDataDirNameError;

    fn from_str(path: &str) -> Result<ParsedDir, ParseFlexDataDirNameError> {
        let split: Vec<String> = path
            .split("/")
            .last()
            .unwrap()
            .split("_")
            .map(|x| x.to_string())
            .collect();

        let uuid = match Uuid::parse_str(split.iter().last().unwrap()) {
            Ok(it) => Ok(it),
            Err(_) => Err(ParseFlexDataDirNameError),
        }?;

        let initial_app_start = match NaiveDateTime::parse_from_str(
            split[0..=1].join("_").split_once(".").unwrap().0,
            "%Y-%m-%d_%H:%M:%S",
        ) {
            Ok(it) => Ok(it),
            Err(e) => {
                Err(ParseFlexDataDirNameError)
            }
        }?;

        let app_version = match AppVersion::from_str(split[4].as_str()) {
            Ok(it) => Ok(it),
            Err(_) => Err(ParseFlexDataDirNameError),
        }?;

        let phone = PhoneModel {
            brand: split[2].clone(),
            model: split[3].clone(),
        };

        Ok(ParsedDir {
            path: path.into(),
            uuid,
            initial_app_start,
            phone,
            app_version,
        })
    }
}

impl TryFrom<DirEntry> for ParsedDir {
    type Error = ParseFlexDataDirNameError;

    fn try_from(value: DirEntry) -> Result<Self, Self::Error> {
        ParsedDir::from_str(value.path().to_string().as_str())
    }
}

impl TryFrom<&DirEntry> for ParsedDir {
    type Error = ParseFlexDataDirNameError;

    fn try_from(value: &DirEntry) -> Result<Self, Self::Error> {
        ParsedDir::from_str(value.path().to_string().as_str())
    }
}

pub trait ToPathBuf {
    fn to_path_buf(&self) -> PathBuf;
}

pub trait ToPathBufVec {
    fn to_path_buf(&self) -> Vec<PathBuf>;
}

impl ToPathBuf for DirEntry {
    fn to_path_buf(&self) -> PathBuf {
        self.path().into()
    }
}

impl ToPathBufVec for Vec<DirEntry> {
    fn to_path_buf(&self) -> Vec<PathBuf> {
        self.iter().map(|x| x.to_path_buf()).collect()
    }
}

pub trait GetPaths {
    fn to_paths(self) -> Vec<PathBuf>;
}

impl<I> GetPaths for I
where
    I: IntoIterator<Item = ParsedDir>,
{
    fn to_paths(self) -> Vec<PathBuf> {
        self.into_iter().map(|x| x.path).collect()
    }
}

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

pub fn parse_subdirs(path: &PathBuf) -> Vec<ParsedDir> {
    list_dirs(path)
        .iter()
        .filter_map(|x| ParsedDir::try_from(x).ok())
        .collect()
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

pub fn find_uuid_dirs<'a>(paths: &Vec<ParsedDir>, uuid: &Uuid) -> HashSet<ParsedDir> {
    HashSet::from_iter(
        paths
            .into_iter()
            .filter(|x| &x.uuid == uuid)
            .map(|x| x.to_owned()),
    )
}

pub fn find_uuids(paths: &Vec<ParsedDir>) -> HashSet<Uuid> {
    HashSet::from_iter(paths.iter().map(|x| x.uuid))
}

pub fn find_inital_app_start(dirs: &HashSet<ParsedDir>) -> Option<NaiveDateTime> {
    dirs.into_iter()
        .map(|x| x.initial_app_start)
        .reduce(|a, b| if a < b { a } else { b })
}

pub fn find_uuids_after(paths: &Vec<ParsedDir>, date: &NaiveDate) -> HashSet<Uuid> {
    HashSet::from_iter(
        paths
            .iter()
            .filter(|x| date < &x.initial_app_start.date())
            .map(|x| x.uuid),
    )
}

pub fn find_sensors(user_dirs: &Vec<PathBuf>) -> HashSet<String> {
    find_sensor_names(get_subdirs(user_dirs, OutputType::logs))
}

fn get_subdirs(user_dirs: &Vec<PathBuf>, subdir_type: OutputType) -> Vec<DirEntry> {
    user_dirs
        .into_iter()
        .map(|dir_entry| {
            let mut path = dir_entry.clone();
            path.push(subdir_type.subdir());
            list_files(path)
        })
        .flatten()
        .collect()
}

pub fn find_first_activity(user_dirs: &Vec<PathBuf>) -> Option<NaiveDateTime> {
    NaiveDateTime::from_timestamp_millis(
        get_subdirs(user_dirs, OutputType::logs)
            .into_iter()
            .map(|x| path_to_begin_timestamp(&x).parse::<i64>().unwrap_or(0))
            .reduce(|a, b| a.max(b))
            .unwrap_or(0),
    )
}

pub fn find_sensor_names(files: Vec<DirEntry>) -> HashSet<String> {
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
