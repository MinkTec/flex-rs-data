use std::{collections::HashSet, str::FromStr};

use crate::{
    fs::{list_files, ParsedDir},
    misc::read_first_line,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use super::TimedData;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyActivities(Vec<TimedData<DailyActivity>>);

impl From<HashSet<ParsedDir>> for DailyActivities {
    fn from(dirs: HashSet<ParsedDir>) -> Self {
        DailyActivities(
            dirs.clone()
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
                        data: DailyActivity::from_str(
                            read_first_line(&file.path()).unwrap().trim(),
                        )
                        .unwrap_or(DailyActivity::Other),
                    })
                })
                .flatten()
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DailyActivity {
    Office,
    Homeoffice,
    Travel,
    Other,
    NA,
    Freetime,
    PhysicalWork,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParsingDailyActivityError;

impl FromStr for DailyActivity {
    type Err = ParsingDailyActivityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "office" => DailyActivity::Office,
            "homeOffice" => DailyActivity::Homeoffice,
            "physicalWork" => DailyActivity::PhysicalWork,
            "freetime" => DailyActivity::Freetime,
            "travel" => DailyActivity::Travel,
            "na" => DailyActivity::NA,
            _ => DailyActivity::Other,
        })
    }
}
