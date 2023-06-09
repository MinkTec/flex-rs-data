use chrono::NaiveDateTime;
use flex_rs_core::dht::CartesianCoordinates;
use polars::{
    prelude::*,
    series::{IntoSeries, Series},
};

pub trait ToSeries {
    fn to_series(&self) -> Series;
}

pub trait ToVec<T> {
    fn to_vec(&self) -> Vec<Option<T>>;
    fn to_vec_unchecked(&self) -> Vec<T>;
}

impl ToVec<Vec<i32>> for ChunkedArray<ListType> {
    fn to_vec(&self) -> Vec<Option<Vec<i32>>> {
        self.into_iter()
            .map(|x| match x {
                Some(x) => Some(x.to_vec().into_iter().map(|x| x.unwrap()).collect()),
                _ => None,
            })
            .collect()
    }

    fn to_vec_unchecked(&self) -> Vec<Vec<i32>> {
        self.into_iter()
            .map(|x| {
                x.unwrap()
                    .to_vec()
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect()
            })
            .collect()
    }
}

impl ToVec<i32> for Series {
    fn to_vec(&self) -> Vec<Option<i32>> {
        match self.i32() {
            Ok(ok) => ok.to_vec(),
            Err(_) => vec![],
        }
    }

    fn to_vec_unchecked(&self) -> Vec<i32> {
        self.to_vec().into_iter().map(|x| x.unwrap()).collect()
    }
}

impl ToVec<i16> for Series {
    fn to_vec(&self) -> Vec<Option<i16>> {
        match self.i32() {
            Ok(ok) => ok
                .into_iter()
                .map(|x| match x {
                    Some(x) => Some(x as i16),
                    _ => None,
                })
                .collect(),
            Err(_) => vec![],
        }
    }

    fn to_vec_unchecked(&self) -> Vec<i16> {
        match self.i32() {
            Ok(ok) => ok.into_iter().map(|x| x.unwrap() as i16).collect(),
            Err(_) => vec![],
        }
    }
}

impl ToVec<i64> for Series {
    fn to_vec(&self) -> Vec<Option<i64>> {
        match self.i64() {
            Ok(ok) => ok.to_vec(),
            Err(_) => match self.datetime() {
                Ok(ok) => ok.into_iter().map(|x| x.into()).collect(),
                Err(_) => vec![],
            },
        }
    }

    fn to_vec_unchecked(&self) -> Vec<i64> {
        match self.i64() {
            Ok(ok) => ok.to_vec().into_iter().map(|x| x.unwrap()).collect(),
            Err(_) => match self.datetime() {
                Ok(ok) => ok.into_iter().map(|x| x.unwrap().into()).collect(),
                Err(_) => vec![],
            },
        }
    }
}

impl ToVec<f64> for Series {
    fn to_vec(&self) -> Vec<Option<f64>> {
        match self.f64() {
            Ok(ok) => ok.to_vec(),
            Err(_) => vec![],
        }
    }

    fn to_vec_unchecked(&self) -> Vec<f64> {
        self.to_vec().into_iter().map(|x| x.unwrap()).collect()
    }
}

impl ToVec<i32> for PolarsResult<&Series> {
    fn to_vec(&self) -> Vec<Option<i32>> {
        match self {
            Ok(series) => series.to_vec(),
            _ => vec![],
        }
    }

    fn to_vec_unchecked(&self) -> Vec<i32> {
        self.to_vec().into_iter().map(|x| x.unwrap()).collect()
    }
}

impl ToVec<i16> for AnyValue<'_> {
    fn to_vec(&self) -> Vec<Option<i16>> {
        match self {
            AnyValue::List(v) => v
                .i32()
                .unwrap()
                .into_iter()
                .map(|x| match x {
                    Some(x) => Some(x as i16),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        }
    }

    fn to_vec_unchecked(&self) -> Vec<i16> {
        match self {
            AnyValue::List(v) => v
                .i32()
                .unwrap()
                .into_iter()
                .map(|x| x.unwrap() as i16)
                .collect(),
            _ => vec![],
        }
    }
}

impl ToVec<f64> for PolarsResult<&Series> {
    fn to_vec(&self) -> Vec<Option<f64>> {
        match self {
            Ok(series) => series.to_vec(),
            _ => vec![],
        }
    }

    fn to_vec_unchecked(&self) -> Vec<f64> {
        self.to_vec().into_iter().map(|x| x.unwrap()).collect()
    }
}

impl ToVec<i64> for PolarsResult<&Series> {
    fn to_vec(&self) -> Vec<Option<i64>> {
        match self {
            Ok(series) => series.to_vec(),
            _ => vec![],
        }
    }

    fn to_vec_unchecked(&self) -> Vec<i64> {
        self.to_vec().into_iter().map(|x| x.unwrap()).collect()
    }
}

impl ToVec<NaiveDateTime> for Logical<DatetimeType, Int64Type> {
    fn to_vec(&self) -> Vec<Option<NaiveDateTime>> {
        self.0
            .into_iter()
            .map(|x| match x {
                Some(i) => NaiveDateTime::from_timestamp_millis(i),
                _ => None,
            })
            .collect()
    }

    fn to_vec_unchecked(&self) -> Vec<NaiveDateTime> {
        self.0
            .into_iter()
            .map(|x| {
                NaiveDateTime::from_timestamp_millis(x.expect("could not unwrap datetime int"))
                    .expect("invalid datetime int")
            })
            .collect()
    }
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

impl<T> ToSeries for Vec<T>
where
    T: ToSeries,
{
    fn to_series(&self) -> Series {
        ListChunked::from_iter(self.into_iter().map(|v| v.to_series())).into_series()
    }
}

impl ToSeries for Vec<&Vec<f64>> {
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
