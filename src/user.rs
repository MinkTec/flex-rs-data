use std::{collections::HashSet, path::PathBuf};

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    df::{create_user_df, ScoreDf},
    fs::{
        find_inital_app_start, find_sensors, find_uuid_dirs, find_uuids_after, list_files,
        parse_subdirs, GetPaths, ParsedDir,
    },
    misc::read_first_line,
    schema::OutputType,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimedData<T> {
    pub time: NaiveDateTime,
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub sensors: HashSet<String>,
    pub active_since: Option<NaiveDateTime>,
    pub dirs: HashSet<ParsedDir>,
}

pub fn gen_users(path: &PathBuf, start_from: Option<NaiveDate>) -> Vec<User> {
    find_uuids_after(
        &parse_subdirs(&path),
        &start_from.unwrap_or(NaiveDate::default()),
    )
    .into_iter()
    .map(|uuid| User::new(uuid))
    .collect()
}

impl User {
    pub fn new(uuid: Uuid) -> User {
        User {
            id: uuid,
            sensors: HashSet::new(),
            active_since: None,
            dirs: HashSet::new(),
        }
    }

    pub fn create_filled_user(paths: &Vec<ParsedDir>, uuid: Uuid) -> User {
        let dirs = find_uuid_dirs(&paths, &uuid);
        User {
            id: uuid,
            sensors: find_sensors(&dirs.clone().to_paths()),
            active_since: dirs.iter().map(|x| x.initial_app_start).reduce(|a, b| {
                if a < b {
                    a
                } else {
                    b
                }
            }),
            dirs: HashSet::from_iter(dirs),
        }
    }

    pub fn fill_user(&mut self, paths: &Vec<ParsedDir>) {
        self.dirs = HashSet::from(find_uuid_dirs(&paths, &self.id));
        self.active_since = find_inital_app_start(&self.dirs);
        self.sensors = find_sensors(&self.dirs.clone().to_paths());
    }

    pub fn get_df(&self, output_type: OutputType, date: Option<NaiveDate>) -> Option<DataFrame> {
        create_user_df(&self.dirs.clone().to_paths(), output_type, date)
    }

    pub fn get_score_df(&self) -> Option<ScoreDf> {
        match self.get_df(OutputType::points, None) {
            Some(df) => Some(ScoreDf::new(df)),
            None => None,
        }
    }

    pub fn get_daily_activities(&self) -> Vec<TimedData<String>> {
        self.dirs
            .clone()
            .into_iter()
            .map(|x| {
                let mut path = x.path;
                path.push("dailyActivity");
                list_files(path).into_iter().map(|file| TimedData {
                    time: NaiveDateTime::parse_from_str(
                        file.file_name()
                            .to_str()
                            .unwrap()
                            .split_once(".")
                            .unwrap()
                            .0,
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .unwrap(),
                    data: read_first_line(&file.path()),
                })
            })
            .flatten()
            .collect()
    }

    pub fn get_activity_blocks(&self) -> Vec<(i64, i64)> {
        let mut v = match self.get_df(OutputType::points, None) {
            Some(df) => df
                .column("t")
                .expect("no t column in df")
                .i64()
                .unwrap()
                .into_iter()
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect(),
            None => vec![],
        };
        v.sort();
        let diff = v.windows(2).map(|x| x[1] - x[0]).collect::<Vec<i64>>();
        let mut last_index: usize = 0;
        let mut activity_blocks = vec![];

        for i in 0..diff.len() {
            if diff[i] > 10000 {
                activity_blocks.push((v[last_index], v[i]));
                last_index = i + 1;
            }
        }
        activity_blocks.push((v[last_index], v.last().unwrap().clone()));
        return activity_blocks;
    }
}
