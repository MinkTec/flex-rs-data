use chrono::NaiveDateTime;
use flex_rs_core::measurement::Measurement;
use polars::{frame::row::Row, lazy::dsl::concat_lst, prelude::*};
use timespan::Timespan;

use crate::{
    misc::{get_num_of_sensors, infer_df_type, timeit},
    schema::OutputType,
    series::ToVec,
};

use super::ColNameGenerator;

#[derive(Debug)]
pub struct RawDf(DataFrame);

impl RawDf {
    pub fn get_measurement_idx(&self, idx: usize) -> Option<Measurement> {
        match self.0.get_row(idx) {
            Ok(row) => Some(RawDf::measurement_from_df_row(row)),
            _ => None,
        }
    }

    pub fn between(&self, ts: Timespan) -> Self {
        let mask = self
            .time()
            .into_iter()
            .map(|x| ts.is_inside(NaiveDateTime::from_timestamp_millis(x.unwrap()).unwrap()))
            .collect();
        RawDf(self.0.filter(&mask).unwrap())
    }

    pub fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.0["t"].datetime().unwrap()
    }
    pub fn left(&self) -> &ChunkedArray<ListType> {
        self.0["left"].list().unwrap()
    }
    pub fn right(&self) -> &ChunkedArray<ListType> {
        self.0["right"].list().unwrap()
    }
    pub fn acc(&self) -> &ChunkedArray<ListType> {
        self.0["acc"].list().unwrap()
    }
    pub fn gyro(&self) -> &ChunkedArray<ListType> {
        self.0["gyro"].list().unwrap()
    }

    pub fn voltage(&self) -> &ChunkedArray<Int32Type> {
        self.0["v"].i32().unwrap()
    }

    fn measurement_from_df_row(row: Row<'_>) -> Measurement {
        let v = row.0;
        Measurement::new_from_split_data(
            v[0].to_vec().into_iter().map(|x| x.unwrap()).collect(),
            v[1].to_vec().into_iter().map(|x| x.unwrap()).collect(),
            v[2].to_vec().into_iter().map(|x| x.unwrap()).collect(),
            v[3].to_vec().into_iter().map(|x| x.unwrap()).collect(),
            match v[4] {
                AnyValue::Int32(v) => v as i16,
                _ => 0,
            },
            match v[5] {
                AnyValue::Int64(v) => v,
                AnyValue::Datetime(v, _, _) => v,
                _ => 0,
            },
        )
    }
}

#[derive(Debug)]
pub struct RawDfConversionError;

impl TryFrom<DataFrame> for RawDf {
    type Error = RawDfConversionError;

    fn try_from(df: DataFrame) -> Result<Self, Self::Error> {
        if let OutputType::raw = infer_df_type(&df) {
            if let Ok(df) = transform_to_new_schema(&df) {
                return Ok(RawDf(df));
            }
        }
        Err(RawDfConversionError)
    }
}

pub fn transform_to_new_schema(df: &DataFrame) -> PolarsResult<DataFrame> {
    let n = get_num_of_sensors(df.shape().1);
    df.clone()
        .lazy()
        .select([
            concat_lst([cols(ColNameGenerator::prefix_n("l", n))])?.alias("left"),
            concat_lst([cols(ColNameGenerator::prefix_n("r", n))])?.alias("right"),
            concat_lst([cols(["x", "y", "z"])])?.alias("acc"),
            concat_lst([cols(["alpha", "beta", "gamma"])])?.alias("gyro"),
            col("v"),
            col("t"),
        ])
        .collect()
}
