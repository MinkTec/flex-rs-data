use chrono::NaiveDate;
use flex_rs_core::dht::CartesianCoordinates;
use flex_rs_core::measurement::{Measurement, SensorPosition};
use polars::prelude::*;

use flex_rs_core;
use uuid::Uuid;

use std::fs::{self, DirEntry, File};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::fs::{
    concat_csv_files, filter_files_by_date, find_uuid_dirs, list_files, parse_subdirs,
};
use crate::misc::{get_num_of_sensors_from_file, infer_df_type, infer_file_type, is_new_schema};
use crate::schema::{generate_flextail_schema, generate_points_schema, OutputType, ScoreDfJS};
use crate::series::ToSeries;

enum TableFormat {
    Csv,
    Arrow,
    Parquet,
}

#[derive(Debug, PartialEq, Eq)]
struct ParseOutputFormatError;

#[derive(Debug)]
pub struct ScoreDf(DataFrame);

impl ScoreDf {
    pub fn new(df: DataFrame) -> ScoreDf {
        ScoreDf(df)
    }

    fn get_unique_days(&self) -> Series {
        self.0
            .column("t")
            .expect("no t column found in groupby day")
            .cast(&DataType::Date)
            .expect("failed to cast datetime to date")
            .unique()
            .unwrap()
    }

    pub fn get_days(&mut self) -> Vec<Result<DataFrame, PolarsError>> {
        self.convert_t_to_time();
        self.0
            .sort_in_place(["t"], false)
            .expect("could not sort frame");

        self.0
            .groupby_with_series(
                vec![self.0.column("t").unwrap().cast(&DataType::Date).unwrap()],
                true,
                true,
            )
            .expect("could no group")
            .groups()
            .expect("could no get groups")
            .column("groups")
            .unwrap()
            .list()
            .unwrap()
            .into_iter()
            .map(|x| self.0.take(x.unwrap().u32().unwrap()))
            .collect()
    }

    fn convert_t_to_time(&mut self) {
        if let Some(df) = convert_i64_to_time(&mut self.0, Some("t")) {
            self.0 = df;
        }
    }

    fn to_js(self) -> ScoreDfJS {
        ScoreDfJS::from(self.0)
    }
}

impl FromStr for TableFormat {
    type Err = ParseOutputFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.splitn(10, '.').last() {
            Some(file_ending) => match file_ending {
                "csv" => Ok(TableFormat::Csv),
                "arrow" => Ok(TableFormat::Arrow),
                "parquet" => Ok(TableFormat::Parquet),
                _ => Err(ParseOutputFormatError),
            },
            None => Err(ParseOutputFormatError),
        }
    }
}

fn read_arrow_file(_path: &PathBuf) -> Option<DataFrame> {
    todo!("arrow format support is not yet implemented");
}

fn read_parquet_file(path: &PathBuf) -> Option<DataFrame> {
    match ParquetReader::new(&mut std::fs::File::open(path).unwrap()).finish() {
        Ok(df) => Some(df),
        Err(e) => {
            println!("could not parse parquet file {e}");
            None
        }
    }
}

fn any_value_to_i16(row: Vec<&AnyValue<'_>>) -> Vec<i16> {
    row.into_iter()
        .map(|x| match x {
            AnyValue::Int32(v) => v.clone() as i16,
            _ => {
                println!("could not convert {}", x);
                0
            }
        })
        .collect()
}

fn measurement_from_df_row(row: Vec<AnyValue<'_>>, n: usize) -> Option<Measurement> {
    let data = row.iter().take(2 * n + 7).collect::<Vec<&AnyValue<'_>>>();
    match row.iter().last().unwrap() {
        AnyValue::Datetime(epoch, _, _) => Some(Measurement::new(
            any_value_to_i16(data),
            Some(epoch.to_owned()),
        )),
        _ => {
            println!("could not create measurement from row: {:?}", row);
            None
        }
    }
}

pub fn transform_to_new_schema(df: &DataFrame) -> Option<DataFrame> {
    let n = (df.get_row(0).unwrap().0.len() - 8) / 2;
    let mut v = vec![];

    for i in 0..df.shape().0 {
        let row = df.get_row(i).unwrap().0;
        match measurement_from_df_row(row, n) {
            Some(m) => v.push(m),
            None => {}
        }
    }

    let position = v
        .iter()
        .map(|x| x.calc_position())
        .collect::<Vec<SensorPosition>>();

    match df!(
        "left" => v.iter().map(|m| &m.left).collect::<Vec<&Vec<i16>>>().to_series(),
        "right" => v.iter().map(|m| &m.right).collect::<Vec<&Vec<i16>>>().to_series(),
        "acc" => v.iter().map(|m| &m.acc).collect::<Vec<&Vec<i16>>>().to_series(),
        "gyro" => v.iter().map(|m| &m.gyro).collect::<Vec<&Vec<i16>>>().to_series(),
        "v" => v.iter().map(|m| &m.v).collect::<Vec<&i16>>().to_series(),
        "t" => v.iter().map(|m| &m.time).collect::<Vec<&Option<i64>>>().to_series(),
        "alpha" => position.iter().map(|m| &m.alpha).collect::<Vec<&Vec<f64>>>().to_series(),
        "beta" => position.iter().map(|m| &m.beta).collect::<Vec<&Vec<f64>>>().to_series(),
        "coords" => position.iter().map(|m| &m.coords).collect::<Vec<&CartesianCoordinates>>().to_series(),
    ) {
        Ok(e) => Some(e),
        Err(e) => {
            println!("failed to create df with error: {}", e);
            None
        }
    }
}

pub fn read_input_file_into_df(path: PathBuf) -> Option<DataFrame> {
    match TableFormat::from_str(&path.to_str().unwrap()) {
        Ok(format) => match format {
            TableFormat::Csv => read_csv_file(&path, infer_file_type(&path)),
            TableFormat::Arrow => read_arrow_file(&path),
            TableFormat::Parquet => read_parquet_file(&path),
        },
        Err(e) => panic!("could not parse input file type {:?}", e),
    }
}

pub fn create_df_from_uuid(
    path: &PathBuf,
    uuid: &Uuid,
    output_type: OutputType,
    date: Option<NaiveDate>,
) -> Option<DataFrame> {
    let folders = find_uuid_dirs(&parse_subdirs(&path), uuid);
    create_user_df(
        &folders.into_iter().map(|x| x.path).collect(),
        output_type,
        date,
    )
}

pub fn create_user_df<'a>(
    folders: &Vec<PathBuf>,
    output_type: OutputType,
    date: Option<NaiveDate>,
) -> Option<DataFrame> {
    let mut files: Vec<DirEntry> = folders
        .iter()
        .map(|x| {
            let mut p = PathBuf::from(x);
            p.push(output_type.subdir());
            list_files(p as PathBuf)
        })
        .flatten()
        .collect();

    if date.is_some() {
        files = filter_files_by_date(files, date.unwrap())
    }

    let new_path = concat_csv_files(files);
    let df = read_csv_file(&new_path, output_type);
    fs::remove_file(new_path).expect("could not delete file");
    return df;
}

pub fn write_df(path: &PathBuf, df: DataFrame) {
    let file = &mut File::create(path).expect("could not create file");
    match TableFormat::from_str(path.to_str().unwrap()) {
        Ok(e) => match e {
            TableFormat::Arrow => todo!("the arrow format writer is not yet implemented"),
            TableFormat::Csv => match CsvWriter::new(file).has_header(false).finish(
                // TODO the polars parser doesn't recognize iso 8601 while parsing
                // therefore the time strings are converted back to i64, which is stupid
                // but otherwise the csv can't be parsed again
                &mut df
                    .clone()
                    .with_column(
                        df.column("t")
                            .expect("did not found column t")
                            .cast(&DataType::Int64)
                            .expect("could not case date to int"),
                    )
                    .expect("could not replace time with int for write"),
            ) {
                Ok(_) => println!("wrote file to {:?}", path),
                Err(_) => println!("failed to write file"),
            },
            TableFormat::Parquet => {
                let mut df = if (!is_new_schema(&df))
                    && match infer_df_type(&df) {
                        OutputType::raw => true,
                        _ => false,
                    } {
                    transform_to_new_schema(&df).unwrap()
                } else {
                    match convert_i64_to_time(&mut df.clone(), None) {
                        Some(df) => df,
                        None => df.clone(),
                    }
                };
                match ParquetWriter::new(file).finish(&mut df) {
                    Ok(_) => println!("wrote df {:?}\n file to {:?}", df, path),
                    Err(_) => println!("failed to write file"),
                }
            }
        },
        Err(_) => todo!(),
    }
}

pub fn convert_i64_to_time(df: &mut DataFrame, column: Option<&str>) -> Option<DataFrame> {
    match df.with_column(
        df.column(column.unwrap_or("t"))
            .expect("did not find column t")
            .cast(&DataType::Datetime(
                polars::prelude::TimeUnit::Milliseconds,
                Some("Europe/Berlin".into()),
            ))
            .expect("could not convert into datetime"),
    ) {
        Ok(e) => Some(e.clone()),
        Err(err) => {
            println!("{}", err);
            None
        }
    }
}

pub fn read_points_csv(path: &PathBuf) -> Option<DataFrame> {
    let reader = CsvReader::from_path(path)
        .unwrap()
        .with_schema(Arc::new(generate_points_schema()))
        .with_ignore_errors(true)
        .has_header(false);
    match reader.finish() {
        Ok(e) => Some(e),
        Err(e) => {
            println!("failed to read points df {}", e);
            None
        }
    }
}

pub fn read_logs_csv(path: &PathBuf) -> Option<DataFrame> {
    let reader = CsvReader::from_path(path)
        .unwrap()
        .with_ignore_errors(true)
        .infer_schema(Some(10))
        .has_header(false);
    reader.finish().ok()
}

pub fn read_raw_csv(path: &PathBuf) -> Option<DataFrame> {
    let schema = Some(generate_flextail_schema(get_num_of_sensors_from_file(
        &path,
    )));
    let reader = CsvReader::from_path(path).unwrap().with_ignore_errors(true);

    let reader = match schema {
        Some(schema) => reader.with_schema(Arc::new(schema)),
        None => reader.infer_schema(Some(100)),
    };

    match reader.has_header(false).finish().as_mut() {
        Ok(e) => {
            //println!("{}", e);
            convert_i64_to_time(e, None)
        }
        Err(err) => {
            println!("{}", err);
            None
        }
    }
}

fn read_csv_file(file: &PathBuf, output_type: OutputType) -> Option<DataFrame> {
    match output_type {
        OutputType::points => read_points_csv(file),
        OutputType::raw => read_raw_csv(file),
        OutputType::logs => read_logs_csv(file),
    }
}

pub fn df_column_to_data_point(
    df: DataFrame,
    time_col: &str,
    value_col: &str,
) -> (Vec<i64>, Vec<f64>) {
    (
        df.column(time_col)
            .unwrap()
            .i64()
            .expect("could not unwrap datetime")
            .to_vec()
            .into_iter()
            .map(|x| x.unwrap())
            .collect(),
        df.column(value_col)
            .unwrap()
            .f64()
            .expect("could not unwrap f64")
            .to_vec()
            .into_iter()
            .map(|x| x.unwrap())
            .collect(),
    )
}
