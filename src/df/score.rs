use std::path::PathBuf;

use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    misc::infer_df_type,
    schema::{OutputType, ScoreDfJS},
    series::ToVec,
};

use derive_more::Deref;

use super::{convert_i64_to_time, create_user_df, read_csv_file};

#[derive(Debug, Deref)]
pub struct ScoreDf(pub DataFrame);

impl ScoreDf {
    pub fn new(df: DataFrame) -> ScoreDf {
        ScoreDf(df)
    }

    fn convert_t_to_time(&mut self) {
        if let Ok(df) = convert_i64_to_time(&mut self.0, None) {
            self.0 = df.to_owned();
        }
    }

    pub fn to_js(self) -> ScoreDfJS {
        ScoreDfJS::from(self.0)
    }

    pub fn summary(&self) -> ScoreDfSummary {
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
    type Error = PolarsError;

    fn try_from(value: DataFrame) -> Result<ScoreDf, Self::Error> {
        if let OutputType::points = infer_df_type(&value) {
            Ok(ScoreDf(value))
        } else {
            Err(PolarsError::SchemaMismatch(
                format!("type infered to {:?}", infer_df_type(&value)).into(),
            ))
        }
    }
}

impl TryFrom<PathBuf> for ScoreDf {
    type Error = PolarsError;

    fn try_from(value: PathBuf) -> PolarsResult<ScoreDf> {
        if value.is_dir() {
            create_user_df(&vec![value], OutputType::points, None)?
        } else {
            read_csv_file(&value, OutputType::points)?
        }
        .to_owned()
        .try_into()
    }
}
