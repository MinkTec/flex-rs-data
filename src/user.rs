pub mod daily_activities;
pub mod feedback;
pub mod metadata;

use crate::{
    df::score::{ScoreDf, ScoreDfSummary},
    feedback::{BackpainFeedback, RectifyFeedback},
    fs::{list_files, MatchStringPattern},
    logs::{find_in_logs, LogEntry},
    misc::{parse_dart_timestring, timeit},
    user::daily_activities::DailyActivities,
};
use anyhow::Result;
use regex::Regex;

use std::{
    cell::RefCell,
    collections::HashSet,
    fs::{read_to_string, DirEntry},
    path::PathBuf,
    str::FromStr,
};

use chrono::NaiveDate;
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use timespan::*;

use crate::{
    df::create_user_df,
    fs::{
        find_inital_app_start, find_sensors, find_uuid_dirs, find_uuids_after, parse_subdirs,
        GetPaths, ParsedDir,
    },
    schema::OutputType,
};

use super::df::time_bound_df::TimeBoundDf;

use self::{feedback::FeedbackType, metadata::UserMetadata};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserScoreSummary {
    pub overall_summary: ScoreDfSummary,
    pub daily_summaries: Vec<DatedData<ScoreDfSummary>>,
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

    pub fn gen_summary(&self) -> Option<UserScoreSummary> {
        if let Some(df) = self.get_score_df() {
            let days = df.get_days();
            Some(UserScoreSummary {
                overall_summary: df.into(),
                daily_summaries: days
                    .into_iter()
                    .filter_map(|x| {
                        if (*x.data).shape().0 > 0 {
                            Some(DatedData {
                                time: x.time,
                                data: (*x.data).into(),
                            })
                        } else {
                            None
                        }
                    })
                    .collect(),
            })
        } else {
            None
        }
    }

    pub fn fill_user(&mut self, paths: &Vec<ParsedDir>) {
        self.dirs = HashSet::from(find_uuid_dirs(&paths, &self.id));
        let mut m = self.metadata.borrow_mut();
        m.initial_app_start = find_inital_app_start(&self.dirs);
        m.sensors = find_sensors(&self.dirs.clone().to_paths());
        if let Some(dir) = self.dirs.iter().last() {
            m.phone = Some(dir.phone.clone());
            m.app_version = Some(dir.app_version.clone());
        };
        m.activities = Some(DailyActivities::from(self.dirs.clone()));
    }

    pub fn get_df(&self, output_type: OutputType, date: Option<NaiveDate>) -> Option<DataFrame> {
        println!("creating user df of type: {:?}", output_type);
        timeit(|| create_user_df(&self.dirs.clone().to_paths(), output_type.clone(), date))
    }

    pub fn get_score_df(&self) -> Option<ScoreDf> {
        match self.get_df(OutputType::points, None) {
            Some(df) => Some(ScoreDf::new(df)),
            None => None,
        }
    }

    pub fn find_in_logs(&self, regex: Regex) -> Vec<LogEntry> {
        find_in_logs(&self.dirs.clone().to_paths(), regex)
    }

    pub fn get_rectify_feedback(&self) -> Option<TimedData<RectifyFeedback>> {
        match self.get_feedback(FeedbackType::Rectify) {
            Some(td) => {
                let data = match RectifyFeedback::from_str(td.data.as_str()) {
                    Ok(f) => f,
                    Err(e) => {
                        println!("failed to parse {} with {:?}", td.data, e);
                        return None;
                    }
                };
                Some(TimedData {
                    time: td.time,
                    data,
                })
            }
            _ => None,
        }
    }

    pub fn get_backpain_feedback(&self) -> Option<TimedData<BackpainFeedback>> {
        match self.get_feedback(FeedbackType::Backpain) {
            Some(td) => {
                let data = match BackpainFeedback::from_str(td.data.as_str()) {
                    Ok(f) => f,
                    Err(e) => {
                        println!("{:?}", e);
                        return None;
                    }
                };
                Some(TimedData {
                    time: td.time,
                    data,
                })
            }
            _ => None,
        }
    }

    fn get_feedback(&self, feedback_type: FeedbackType) -> Option<TimedData<String>> {
        let mut candidates = self
            .dirs
            .clone()
            .to_paths()
            .iter()
            .map(|x| {
                let mut x = x.clone();
                x.push("feedback");
                list_files(x)
            })
            .flatten()
            .collect::<Vec<DirEntry>>()
            .filter_pattern(feedback_type.matcher());

        candidates.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        match candidates.last() {
            Some(e) => match read_to_string(e.path()) {
                Ok(string) => {
                    let time = parse_dart_timestring(
                        e.path()
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string()
                            .split_once("_")
                            .unwrap()
                            .1,
                    )
                    .unwrap();
                    Some(TimedData { time, data: string })
                }
                _ => None,
            },
            _ => None,
        }
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
