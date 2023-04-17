use chrono::NaiveDateTime;
use flex_rs_core::dht::CartesianCoordinates;
use polars::{
    prelude::*,
    series::{IntoSeries, Series},
};

pub trait ToSeries {
    fn to_series(&self) -> Series;
}

// converts to time (expects unix timestamps)
impl ToSeries for Vec<&Option<i64>> {
    fn to_series(&self) -> Series {
        DatetimeChunked::from_naive_datetime_options(
            "time",
            self.into_iter()
                .map(|x| NaiveDateTime::from_timestamp_millis(x.unwrap_or(0))),
            TimeUnit::Milliseconds,
        )
        .into_series()
    }
}

impl ToSeries for Vec<f64> {
    fn to_series(&self) -> Series {
        Float64Chunked::from_vec("", self.into_iter().map(|x| x.to_owned()).collect()).into_series()
    }
}

impl ToSeries for Vec<&Vec<f64>> {
    fn to_series(&self) -> Series {
        ListChunked::from_iter(self.into_iter().map(|v| v.to_series())).into_series()
    }
}

impl ToSeries for Vec<Vec<f64>> {
    fn to_series(&self) -> Series {
        ListChunked::from_iter(self.into_iter().map(|v| v.to_series())).into_series()
    }
}

impl ToSeries for Vec<Vec<i16>> {
    fn to_series(&self) -> Series {
        ListChunked::from_iter(self.into_iter().map(|v| v.to_series())).into_series()
    }
}

impl ToSeries for Vec<i16> {
    fn to_series(&self) -> Series {
        Int32Chunked::from_vec("", self.into_iter().map(|x| x.to_owned() as i32).collect())
            .into_series()
    }
}

impl ToSeries for &CartesianCoordinates {
    fn to_series(&self) -> Series {
        ListChunked::from_iter(vec![
            Float64Chunked::from_vec("x", self.x.to_owned()).into_series(),
            Float64Chunked::from_vec("y", self.y.to_owned()).into_series(),
            Float64Chunked::from_vec("z", self.z.to_owned()).into_series(),
        ])
        .into_series()
    }
}

impl ToSeries for Vec<&CartesianCoordinates> {
    fn to_series(&self) -> Series {
        ListChunked::from_iter(self.into_iter().map(|v| v.to_series())).into_series()
    }
}

impl ToSeries for Vec<&i16> {
    fn to_series(&self) -> Series {
        Int32Chunked::from_vec(
            "",
            self.into_iter()
                .map(|x| x.to_owned().to_owned() as i32)
                .collect(),
        )
        .into_series()
    }
}

impl ToSeries for Vec<&Vec<i16>> {
    fn to_series(&self) -> Series {
        ListChunked::from_iter(self.into_iter().map(|v| v.to_series())).into_series()
    }
}
