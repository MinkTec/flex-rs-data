use flex_rs_core::{
    measurement::Measurement, sensor_angles::calc_angles_with_default_params,
    FlextailPositionContainer,
};
use polars::{frame::row::Row, lazy::dsl::concat_lst, prelude::*};

use crate::{
    misc::{get_num_of_sensors, infer_df_type},
    schema::OutputType,
    series::{ToSeries, ToVec},
};

use derive_more::Deref;

use super::ColNameGenerator;

pub fn transform_to_new_schema(df: &DataFrame) -> PolarsResult<DataFrame> {
    dbg!(df.shape());
    if df.shape().1 == 6 {
        Ok(df.to_owned())
    } else {
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

#[derive(Debug, Deref)]
pub struct RawDf(pub DataFrame);

impl RawDf {
    pub fn get_measurement_idx(&self, idx: usize) -> Option<Measurement> {
        match self.0.get_row(idx) {
            Ok(row) => Some(RawDf::measurement_from_df_row(row)),
            _ => None,
        }
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

    pub fn calc_angles(&self) -> Vec<FlextailPositionContainer> {
        self.left()
            .into_iter()
            .zip(self.right())
            .map(|x| {
                calc_angles_with_default_params(
                    &x.0.unwrap().to_vec_unchecked(),
                    &x.1.unwrap().to_vec_unchecked(),
                )
            })
            .collect()
    }

    pub fn with_movement_score(&self) -> RawDf {
        let mut v = vec![0.0; 15];
        v.append(&mut self.calc_movement_score(15));

        RawDf(
            self.0
                .clone()
                .replace_or_add("movement", v.to_series())
                .unwrap()
                .clone(),
        )
    }

    pub fn calc_movement_score(&self, n: usize) -> Vec<f64> {
        self.acc().to_vec_unchecked()[..]
            .windows(2)
            .map(|x| [x[1][0] - x[0][0], x[1][1] - x[0][1], x[1][2] - x[0][2]].map(|x| x.abs()))
            .collect::<Vec<[i32; 3]>>()[..]
            .windows(n)
            .map(|v| {
                let mut x = 0;
                let mut y = 0;
                let mut z = 0;
                for i in 0..n {
                    x += v[i][0];
                    y += v[i][1];
                    z += v[i][2];
                }
                x as f64 / n as f64 + y as f64 / n as f64 + z as f64 / n as f64 / 3.0
            })
            .collect()
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
