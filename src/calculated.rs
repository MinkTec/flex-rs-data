use polars::prelude::*;

pub struct CalculatedDf(DataFrame);

impl CalculatedDf {
    pub fn between(&self, ts: Timespan) -> Self {
        let mask = self
            .time()
            .into_iter()
            .map(|x| ts.is_inside(NaiveDateTime::from_timestamp_millis(x.unwrap()).unwrap()))
            .collect();
        RawDf(self.0.filter(&mask).unwrap())
    }
}
