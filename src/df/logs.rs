use polars::prelude::*;

#[derive(Debug, derive_more::Deref)]
pub struct LogsDf(DataFrame);

impl LogsDf {
    pub fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.0["t"].datetime().unwrap()
    }
}
