use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use timespan::Timespan;

use super::{raw::RawDf, score::ScoreDf};

pub trait TimeBoundDf {
    fn between(&self, ts: Timespan) -> Self;
    fn day(&self, date: NaiveDate) -> Self;
    fn timespan(&self) -> Option<Timespan>;
}

impl TimeBoundDf for RawDf {
    fn between(&self, ts: Timespan) -> Self {
        self.between(ts)
    }

    fn day(&self, date: NaiveDate) -> Self {
        self.between(Timespan {
            begin: date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
            end: date.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap()),
        })
    }

    fn timespan(&self) -> Option<Timespan> {
        todo!();
    }
}

impl TimeBoundDf for ScoreDf {
    fn between(&self, ts: Timespan) -> Self {
        self.between(ts)
    }

    fn day(&self, date: NaiveDate) -> Self {
        self.between(Timespan {
            begin: date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
            end: date.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap()),
        })
    }

    fn timespan(&self) -> Option<Timespan> {
        if let Some(begin) = self.time_col().min() {
            if let Some(end) = self.time_col().max() {
                return Some(Timespan {
                    begin: NaiveDateTime::from_timestamp_millis(begin).unwrap(),
                    end: NaiveDateTime::from_timestamp_millis(end).unwrap(),
                });
            }
        }
        None
    }
}

pub struct TimeBoundDfEmpty;
