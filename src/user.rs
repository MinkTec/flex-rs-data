pub mod daily_activities;
pub mod feedback;
pub mod metadata;
pub mod stats;

use crate::df::raw::RawDf;
use crate::{
    df::score::{ScoreDf, ScoreDfSummary},
    feedback::{BackpainFeedback, RectifyFeedback},
    fs::{list_files, MatchStringPattern},
    logs::{find_in_logs, LogEntry},
    misc::parse_dart_timestring,
    user::daily_activities::DailyActivities,
};
use anyhow::Result;
use rayon::prelude::*;
use regex::Regex;

use std::sync::{Arc, Mutex};
use std::{
    cell::RefCell,
    collections::HashSet,
    fs::{read_to_string, DirEntry},
    path::PathBuf,
    str::FromStr,
};

use chrono::NaiveDate;
use polars::prelude::{DataFrame, PolarsResult};
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

use self::{feedback::FeedbackType, metadata::UserMetadata};
use super::df::time_bound_df::TimeBoundDf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserScoreSummary {
    pub overall_summary: ScoreDfSummary,
    pub daily_summaries: Vec<DatedData<ScoreDfSummary>>,
}

pub type Memo<T> = Arc<Mutex<RefCell<Option<T>>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub dirs: HashSet<ParsedDir>,
    pub metadata: RefCell<UserMetadata>,
    #[serde(skip)]
    raw_df: Memo<RawDf>,
    #[serde(skip)]
    last_raw_df_date: Memo<NaiveDate>,
    #[serde(skip)]
    score_df: Memo<ScoreDf>,
}

impl Clone for User {
    fn clone(&self) -> Self {
        return User {
            id: self.id.clone(),
            dirs: self.dirs.clone(),
            metadata: self.metadata.clone(),
            raw_df: Arc::new(Mutex::new(RefCell::new(None))),
            score_df: Arc::new(Mutex::new(RefCell::new(None))),
            last_raw_df_date: Arc::new(Mutex::new(RefCell::new(None))),
        };
    }
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
            raw_df: Arc::new(Mutex::new(RefCell::new(None))),
            score_df: Arc::new(Mutex::new(RefCell::new(None))),
            last_raw_df_date: Arc::new(Mutex::new(RefCell::new(None))),
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
            raw_df: Arc::new(Mutex::new(RefCell::new(None))),
            score_df: Arc::new(Mutex::new(RefCell::new(None))),
            last_raw_df_date: Arc::new(Mutex::new(RefCell::new(None))),
        }
    }

    pub fn gen_summary(&self) -> Option<UserScoreSummary> {
        let df = self.get_score_df();
        let summaries = df
            .get_days()
            .par_iter()
            .filter_map(|x| {
                if (*x.data).shape().0 > 0 {
                    Some(DatedData {
                        time: x.time,
                        data: x.data.summary(),
                    })
                } else {
                    None
                }
            })
            .collect::<Vec<DatedData<ScoreDfSummary>>>();

        Some(UserScoreSummary {
            overall_summary: df.summary(),
            daily_summaries: summaries,
        })
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

    pub fn get_df(
        &self,
        output_type: OutputType,
        date: Option<NaiveDate>,
    ) -> PolarsResult<DataFrame> {
        create_user_df(&self.dirs.clone().to_paths(), output_type.clone(), date)
    }

    pub fn get_score_df(&self) -> ScoreDf {
        let guard = self.score_df.lock().unwrap();
        let mut cache = guard.borrow_mut();

        if cache.is_none() {
            *cache = Some(ScoreDf(self.get_df(OutputType::points, None).unwrap()));
        }

        ScoreDf(cache.as_deref().unwrap().clone())
    }

    pub fn get_raw_df(&self, date: Option<NaiveDate>) -> RawDf {
        let guard = self.raw_df.lock().unwrap();
        let mut cache = guard.borrow_mut();

        if cache.is_none() || *self.last_raw_df_date.lock().unwrap().borrow() != date {
            *cache = Some(RawDf(self.get_df(OutputType::raw, date).unwrap()));
        }

        RawDf(cache.as_deref().unwrap().clone())
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

        Ok(())
    }

    pub fn get_activity_blocks(&self) -> Vec<Timespan> {
        match self.get_df(OutputType::points, None) {
            Ok(df) => ScoreDf(df).get_activity_timespans(300000),
            Err(_) => vec![],
        }
    }
}

impl Into<ScoreDfSummary> for Vec<ScoreDfSummary> {
    fn into(self) -> ScoreDfSummary {
        let iter = self.iter();
        ScoreDfSummary {
            average_score: iter.clone().map(|x| x.average_score).sum::<f64>() / (self.len() as f64),
            duration: iter.clone().map(|x| x.duration).sum(),
            min: iter
                .clone()
                .map(|x| x.min)
                .reduce(|a, b| if a > b { b } else { a })
                .unwrap(),
            max: iter
                .clone()
                .map(|x| x.max)
                .reduce(|a, b| if a < b { b } else { a })
                .unwrap(),
        }
    }
}
