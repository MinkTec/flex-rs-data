use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    misc::infer_df_type,
    schema::{OutputType, ScoreDfJS},
    series::ToVec,
};

use derive_more::Deref;

use super::convert_i64_to_time;

#[derive(Debug, Deref)]
pub struct ScoreDf(pub DataFrame);

impl ScoreDf {
    pub fn new(df: DataFrame) -> ScoreDf {
        ScoreDf(df)
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

    pub fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.0["t"]
            .datetime()
            .expect("could not get time series score df")
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

#[derive(Debug)]
pub struct ScoreDfConversionError;

impl TryFrom<DataFrame> for ScoreDf {
    type Error = ScoreDfConversionError;

    fn try_from(value: DataFrame) -> Result<ScoreDf, Self::Error> {
        if let OutputType::points = infer_df_type(&value) {
            Ok(ScoreDf(value))
        } else {
            Err(ScoreDfConversionError)
        }
    }
}
