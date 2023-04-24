pub mod daily_activities;
pub mod metadata;

use crate::{fs::list_files, misc::timeit, user::daily_activities::DailyActivities};
use anyhow::Result;

use std::{cell::RefCell, collections::HashSet, fs::DirEntry, path::PathBuf};

use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    df::{create_user_df, ScoreDf},
    fs::{
        find_inital_app_start, find_sensors, find_uuid_dirs, find_uuids_after, parse_subdirs,
        GetPaths, ParsedDir,
    },
    schema::OutputType,
};

use self::metadata::UserMetadata;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimedData<T> {
    pub time: NaiveDateTime,
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub dirs: HashSet<ParsedDir>,
    pub metadata: RefCell<UserMetadata>,
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
            dirs: HashSet::new(),
            metadata: RefCell::new(UserMetadata::new()),
        }
    }

    pub fn create_filled_user(paths: &Vec<ParsedDir>, uuid: Uuid) -> User {
        let dirs = find_uuid_dirs(&paths, &uuid);
        User {
            id: uuid,
            dirs: HashSet::from_iter(dirs.clone()),
            metadata: RefCell::new(
                UserMetadata::new()
                    .with_sensors(find_sensors(&dirs.clone().to_paths()))
                    .with_active_since(dirs.iter().map(|x| x.initial_app_start).reduce(|a, b| {
                        if a < b {
                            a
                        } else {
                            b
                        }
                    })),
            ),
        }
    }

    pub fn fill_user(&mut self, paths: &Vec<ParsedDir>) {
        self.dirs = HashSet::from(find_uuid_dirs(&paths, &self.id));
        let mut m = self.metadata.borrow_mut();
        m.initial_app_start = find_inital_app_start(&self.dirs);
        m.sensors = find_sensors(&self.dirs.clone().to_paths());
        if let Some(dir) = self.dirs.iter().next() {
            m.phone = Some(dir.phone.clone());
            m.app_version = Some(dir.app_version.clone());
        };
        m.activities = Some(DailyActivities::from(self.dirs.clone()));
    }

    pub fn get_df(&self, output_type: OutputType, date: Option<NaiveDate>) -> Option<DataFrame> {
        println!("creating user df");
        timeit(|| create_user_df(&self.dirs.clone().to_paths(), output_type.clone(), date))
    }

    pub fn get_score_df(&self) -> Option<ScoreDf> {
        match self.get_df(OutputType::points, None) {
            Some(df) => Some(ScoreDf::new(df)),
            None => None,
        }
    }

    fn get_feedback(&self) -> Vec<DirEntry> {
        self.dirs
            .clone()
            .to_paths()
            .iter()
            .map(|x| {
                let mut x = x.clone();
                x.push("feedback");
                list_files(x)
            })
            .flatten()
            .collect()
    }

    pub fn get_rectify_feedback(&self) -> Option<String> {
        /*
        self.get_feedback().iter().filter(|x| {
            x.file_name()
                .to_str()
                .expect("could not unwrap filename")
                .to_string()
                .contains("rectify_")
        })
        */
        todo!()
    }

    pub fn get_backpain_feedback(&self) -> Option<String> {
        todo!()
    }

    pub fn get_daily_activities(&self) -> DailyActivities {
        self.metadata
            .borrow()
            .clone()
            .activities
            .unwrap_or(DailyActivities::from(self.dirs.clone()))
    }

    pub fn set_metadata(&self) -> Result<()> {
        let mut m = self.metadata.try_borrow_mut()?;
        if m.sensors.len() == 0 {
            m.sensors = find_sensors(&self.dirs.clone().to_paths())
        }

        m.phone = match m.phone.clone() {
            Some(phone) => Some(phone),
            _ => match self.dirs.iter().next() {
                Some(dir) => Some(dir.phone.clone()),
                _ => None,
            },
        };

        m.app_version = match m.app_version.clone() {
            Some(app_version) => Some(app_version),
            _ => match self.dirs.iter().next() {
                Some(dir) => Some(dir.app_version.clone()),
                _ => None,
            },
        };

        m.activities = Some(self.get_daily_activities());

        //m.active_since = Some()

        Ok(())
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
