use polars::prelude::*;

#[derive(Debug, derive_more::Deref)]
pub struct GenericTimeBoundDf(DataFrame);

impl GenericTimeBoundDf {
    pub fn new(df: DataFrame) -> GenericTimeBoundDf {
        GenericTimeBoundDf(df)
    }

    pub fn time(&self) -> &Logical<DatetimeType, Int64Type> {
        self.0["t"].datetime().unwrap()
    }
}

impl TryFrom<DataFrame> for GenericTimeBoundDf {
    type Error = PolarsError;

    fn try_from(value: DataFrame) -> Result<Self, Self::Error> {
        if value.get_column_names().into_iter().any(|x| x == "t") {
            Ok(GenericTimeBoundDf(value))
        } else {
            Err(PolarsError::SchemaMismatch("df has no t column".into()))
        }
    }
}
