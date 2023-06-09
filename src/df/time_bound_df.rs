use std::{fmt::Debug, ops::Deref};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::*;

use timespan::{DatedData, Timespan};

use crate::schema::OutputType;

use super::{generic::GenericTimeBoundDf, logs::LogsDf, raw::RawDf, score::ScoreDf};

pub trait TimeBoundDf {
    fn day(&self, date: NaiveDate) -> Self;
    fn timespan(&self) -> Option<Timespan>;
    fn get_activity_timespans(&self, threshold: i64) -> Vec<Timespan>;
    fn split_into_time_chunks(&self, duration: i64) -> Vec<Box<Self>>;
    fn get_days(&self, min_length: Option<usize>) -> Vec<DatedData<Box<Self>>>;
}

pub trait TimeColumn {
    fn time(&self) -> &Logical<DatetimeType, Int64Type>;
}

impl TimeColumn for RawDf {
    fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.time()
    }
}

impl TimeColumn for ScoreDf {
    fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.time()
    }
}

impl TimeColumn for LogsDf {
    fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.time()
    }
}

impl TimeColumn for GenericTimeBoundDf {
    fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.time()
    }
}

pub trait Between {
    fn between(&self, ts: Timespan) -> Self;
}

impl<F> Between for F
where
    F: TimeColumn + TryFrom<DataFrame> + Deref<Target = DataFrame>,
{
    fn between(&self, ts: Timespan) -> Self {
        let mask = self
            .time()
            .into_iter()
            .map(|x| ts.is_inside(NaiveDateTime::from_timestamp_millis(x.unwrap()).unwrap()))
            .collect();
        match self.filter(&mask).unwrap().try_into() {
            Ok(df) => df,
            _ => panic!("could not convert df after between"),
        }
    }
}

impl<T> TimeBoundDf for T
where
    T: Debug + Between + TimeColumn + Deref<Target = DataFrame>,
{
    fn day(&self, date: NaiveDate) -> Self {
        self.between(Timespan {
            begin: date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
            end: date.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap()),
        })
    }

    fn timespan(&self) -> Option<Timespan> {
        if let Some(begin) = self.time().min() {
            if let Some(end) = self.time().max() {
                return Some(Timespan {
                    begin: NaiveDateTime::from_timestamp_millis(
                        begin.max(
                            NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap()
                                .timestamp_millis(),
                        ),
                    )
                    .unwrap(),
                    end: NaiveDateTime::from_timestamp_millis(
                        end.min(
                            NaiveDate::from_ymd_opt(2024, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap()
                                .timestamp_millis(),
                        ),
                    )
                    .unwrap(),
                });
            }
        }
        None
    }

    /// threshold is in ms
    fn get_activity_timespans(&self, threshold: i64) -> Vec<Timespan> {
        let mut v = self
            .time()
            .to_vec()
            .into_iter()
            .map(|x| x.unwrap())
            .collect::<Vec<i64>>();
        v.sort();
        let diff = v.windows(2).map(|x| x[1] - x[0]).collect::<Vec<i64>>();

        let mut last_index: usize = 0;
        let mut activity_blocks = vec![];

        for i in 0..diff.len() {
            if diff[i] > threshold {
                activity_blocks.push((v[last_index], v[i]).into());
                last_index = i + 1;
            }
        }
        activity_blocks.push((v[last_index], v.last().unwrap().clone()).into());
        activity_blocks
    }

    fn split_into_time_chunks(&self, duration: i64) -> Vec<Box<Self>> {
        self.get_activity_timespans(duration)
            .into_iter()
            .map(|x| Box::new(self.between(x)))
            .collect()
    }

    fn get_days(&self, min_length: Option<usize>) -> Vec<DatedData<Box<Self>>> {
        match self.timespan() {
            Some(spans) => spans
                .days()
                .into_iter()
                .filter_map(|x| {
                    let r = DatedData {
                        time: x,
                        data: Box::new(self.between(x.into())),
                    };
                    if r.data.height() > min_length.unwrap_or(0) {
                        Some(r)
                    } else {
                        None
                    }
                })
                .collect(),
            None => vec![],
        }
    }
}

trait DataFrameType {
    fn data_frame_type() -> OutputType;
}

impl DataFrameType for RawDf {
    fn data_frame_type() -> OutputType {
        OutputType::raw
    }
}
impl DataFrameType for ScoreDf {
    fn data_frame_type() -> OutputType {
        OutputType::points
    }
}

pub struct TimeBoundDfEmpty;
