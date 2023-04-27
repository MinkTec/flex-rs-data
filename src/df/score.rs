use chrono::NaiveDateTime;
use polars::prelude::{DataFrame, DataType, PolarsError, Series, SeriesTrait};
use serde::{Deserialize, Serialize};

use crate::{schema::ScoreDfJS, series::ToVec};

use super::convert_i64_to_time;

#[derive(Debug)]
pub struct ScoreDf(DataFrame);

impl ScoreDf {
    pub fn new(df: DataFrame) -> ScoreDf {
        ScoreDf(df)
    }

    fn _get_unique_days(&self) -> Series {
        self.0
            .column("t")
            .expect("no t column found in groupby day")
            .cast(&DataType::Date)
            .expect("failed to cast datetime to date")
            .unique()
            .unwrap()
    }

    pub fn get_days(&mut self) -> Vec<Result<ScoreDf, PolarsError>> {
        self.convert_t_to_time();
        self.0
            .sort_in_place(["t"], false)
            .expect("could not sort frame");

        self.0
            .groupby_with_series(
                vec![self.0.column("t").unwrap().cast(&DataType::Date).unwrap()],
                true,
                true,
            )
            .expect("could no group")
            .groups()
            .expect("could no get groups")
            .column("groups")
            .unwrap()
            .list()
            .unwrap()
            .into_iter()
            .map(|x| match self.0.take(x.unwrap().u32().unwrap()) {
                Ok(df) => Ok(ScoreDf(df)),
                Err(e) => Err(e),
            })
            .collect()
    }

    fn convert_t_to_time(&mut self) {
        if let Some(df) = convert_i64_to_time(&mut self.0, Some("t")) {
            self.0 = df;
        }
    }

    pub fn to_js(self) -> ScoreDfJS {
        ScoreDfJS::from(self.0)
    }

    fn summary(self) -> ScoreDfSummary {
        let col = self.0.column("score").unwrap();

        ScoreDfSummary {
            average_score: col.mean().unwrap_or(50.0),
            duration: col.len() as u32,
            max: col.max().unwrap_or(0.0),
            min: col.min().unwrap_or(0.0),
        }
    }

    fn score(&self) -> Vec<Option<f64>> {
        self.0.column("score").to_vec()
    }

    pub fn begin_and_end(&self) -> (NaiveDateTime, NaiveDateTime) {
        let col = self.0.column("t").unwrap();
        (
            NaiveDateTime::from_timestamp_millis(col.min().unwrap()).unwrap(),
            NaiveDateTime::from_timestamp_millis(col.max().unwrap()).unwrap(),
        )
    }

    fn time(&self) -> Vec<Option<NaiveDateTime>> {
        self.0
            .column("t")
            .to_vec()
            .into_iter()
            .map(|x| match x {
                Some(x) => NaiveDateTime::from_timestamp_millis(x),
                None => None,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreDfSummary {
    pub average_score: f64,
    // in seconds
    pub duration: u32,
    pub min: f64,
    pub max: f64,
}

impl Into<ScoreDfSummary> for ScoreDf {
    fn into(self) -> ScoreDfSummary {
        self.summary()
    }
}

