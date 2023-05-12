use polars::prelude::{DataFrame, DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use timespan::DatedData;

use crate::{
    df::{raw::RawDf, score::ScoreDf},
    series::ToVec,
};

pub trait ToJS<T> {
    fn to_js(&self) -> T
    where
        T: Serialize;
}

impl ToJS<ScoreDfJS> for ScoreDf {
    fn to_js(&self) -> ScoreDfJS {
        self.0.clone().into()
    }
}

impl ToJS<RawDfJS> for RawDf {
    fn to_js(&self) -> RawDfJS
    where
        RawDfJS: Serialize,
    {
        RawDf(self.0.clone()).into()
    }
}

impl ToJS<Vec<DatedData<ScoreDfJS>>> for Vec<DatedData<Box<ScoreDf>>> {
    fn to_js(&self) -> Vec<DatedData<ScoreDfJS>> {
        self.into_iter()
            .map(|x| DatedData {
                time: x.time,
                data: ScoreDf(x.data.clone()).to_js(),
            })
            .collect()
    }
}

impl ToJS<Vec<DatedData<RawDfJS>>> for Vec<DatedData<Box<RawDf>>> {
    fn to_js(&self) -> Vec<DatedData<RawDfJS>> {
        self.into_iter()
            .map(|x| DatedData {
                time: x.time,
                data: RawDf(x.data.clone()).to_js(),
            })
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScoreDfJS {
    pub t: Vec<Option<i64>>,
    pub score: Vec<Option<f64>>,
    pub posture: Vec<Option<f64>>,
    pub movement: Vec<Option<f64>>,
    pub activity: Vec<String>,
}

impl From<DataFrame> for ScoreDfJS {
    fn from(df: DataFrame) -> Self {
        ScoreDfJS {
            t: df.column("t").to_vec(),
            score: df.column("score").to_vec(),
            posture: df.column("posture").to_vec(),
            movement: df.column("movement").to_vec(),
            activity: match df.column("activity") {
                Ok(col) => match col.utf8() {
                    Ok(ok) => ok
                        .into_iter()
                        .map(|x| x.unwrap_or("").to_string())
                        .collect(),
                    Err(_) => vec![],
                },
                Err(_) => vec![],
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RawDfJS {
    pub t: Vec<Option<i64>>,
    pub left: Vec<Option<Vec<i32>>>,
    pub right: Vec<Option<Vec<i32>>>,
    pub acc: Vec<Option<Vec<i32>>>,
    pub gyro: Vec<Option<Vec<i32>>>,
    pub v: Vec<Option<i32>>,
    pub movement: Option<Vec<Option<f64>>>,
}

impl From<RawDf> for RawDfJS {
    fn from(df: RawDf) -> Self {
        RawDfJS {
            t: df.time().to_vec(),
            left: df.left().to_vec(),
            right: df.right().to_vec(),
            acc: df.acc().to_vec(),
            gyro: df.gyro().to_vec(),
            v: df.voltage().to_vec(),
            movement: match df.0.column("movement") {
                Ok(series) => Some(series.to_vec()),
                Err(_) => None,
            },
        }
    }
}

pub fn generate_flextail_schema(n: usize) -> Schema {
    let mut fields: Vec<Field> = vec![];
    let mut left = gen_sensor_fields(n, "l");
    let mut right = gen_sensor_fields(n, "r");
    fields.append(&mut left);
    fields.append(&mut right);
    fields.append(&mut gen_non_senosor_fields());
    Schema::from_iter(fields)
}

pub fn generate_points_schema() -> Schema {
    Schema::from_iter(vec![
        Field::new("t", DataType::Int64),
        Field::new("score", DataType::Float64),
        Field::new("posture", DataType::Float64),
        Field::new("movement", DataType::Float64),
        Field::new("activity", DataType::Utf8),
    ])
}

fn gi16f(name: &str) -> Field {
    Field::new(name, DataType::Int32)
}

fn gen_non_senosor_fields() -> Vec<Field> {
    vec![
        gi16f("x"),
        gi16f("y"),
        gi16f("z"),
        gi16f("alpha"),
        gi16f("beta"),
        gi16f("gamma"),
        gi16f("v"),
        Field::new("t", DataType::Int64),
    ]
}

fn gen_sensor_fields(n: usize, prefix: &str) -> Vec<Field> {
    (1..=n)
        .into_iter()
        .map(|x| gi16f(format!("{}{}", prefix, x).as_str()))
        .collect()
}

#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum OutputType {
    points,
    raw,
    logs,
}

impl OutputType {
    pub fn subdir(&self) -> PathBuf {
        match self {
            OutputType::points => PathBuf::from("points"),
            OutputType::raw => PathBuf::from("raw"),
            OutputType::logs => PathBuf::from("logs"),
        }
    }

    pub fn schema(&self, length: Option<usize>) -> Option<Schema> {
        let time_field = Field::new(
            "t",
            DataType::Datetime(polars::prelude::TimeUnit::Milliseconds, None),
        );

        match self {
            OutputType::points => Some(Schema::from_iter(vec![
                time_field,
                Field::new("score", DataType::Float64),
                Field::new("posture", DataType::Float64),
                Field::new("movement", DataType::Float64),
                Field::new("activity", DataType::Utf8),
            ])),
            OutputType::raw => Some(generate_flextail_schema(length.unwrap_or(18))),
            OutputType::logs => None,
        }
    }
}
