pub mod generic;
pub mod logs;
pub mod raw;
pub mod score;
pub mod time_bound_df;

use chrono::NaiveDate;
use polars::prelude::*;

use uuid::Uuid;

use std::fs::{self, File};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::fs::{
    concat_csv_files, filter_files_by_date, find_uuid_dirs, list_files, parse_subdirs,
};
use crate::misc::{
    get_num_of_sensors_from_file, infer_df_type, infer_file_type, is_new_schema,
    parse_dart_timestring_short, read_first_n_chars,
};
use crate::schema::{generate_flextail_schema, generate_points_schema, OutputType};

use self::raw::transform_to_new_schema;

#[derive(Debug)]
enum TableFormat {
    Csv,
    Arrow,
    Parquet,
}

#[derive(Debug, PartialEq, Eq)]
struct ParseOutputFormatError;

impl FromStr for TableFormat {
    type Err = ParseOutputFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split('.');
        match dbg!(if parts.clone().count() == 1 {
            None
        } else {
            Some(parts.last().unwrap())
        }) {
            Some(file_ending) => match file_ending {
                "csv" => Ok(TableFormat::Csv),
                "arrow" => Ok(TableFormat::Arrow),
                "parquet" => Ok(TableFormat::Parquet),
                _ => Err(ParseOutputFormatError),
            },
            None => match dbg!(read_first_n_chars(&s.to_string().into()).as_str()) {
                "PAR1" => Ok(TableFormat::Parquet),
                "ARR1" => Ok(TableFormat::Arrow),
                _ => Ok(TableFormat::Csv),
            },
        }
    }
}

struct ColNameGenerator;

impl ColNameGenerator {
    pub fn prefix_n(prefix: &str, n: usize) -> Vec<String> {
        (1..=n)
            .into_iter()
            .map(|x| format!("{}{}", prefix, x))
            .collect()
    }
}

fn read_arrow_file(_path: &PathBuf) -> PolarsResult<DataFrame> {
    todo!("arrow format support is not yet implemented");
}

fn read_parquet_file(path: &PathBuf) -> PolarsResult<DataFrame> {
    ParquetReader::new(&mut std::fs::File::open(path).unwrap())
        .read_parallel(ParallelStrategy::Auto)
        .finish()
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

pub fn read_input_file_into_df(path: PathBuf) -> PolarsResult<DataFrame> {
    match dbg!(TableFormat::from_str(&path.to_str().unwrap())) {
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
) -> PolarsResult<DataFrame> {
    create_user_df(
        &find_uuid_dirs(&parse_subdirs(&path), uuid)
            .into_iter()
            .map(|x| x.path)
            .collect(),
        output_type,
        date,
    )
}

pub fn create_user_df_from_files(
    files: Vec<PathBuf>,
    output_type: OutputType,
    date: Option<NaiveDate>,
) -> PolarsResult<DataFrame> {
    let files = if date.is_some() {
        filter_files_by_date(&files, date.unwrap())
    } else {
        files
    };

    let new_path = concat_csv_files(&files);
    let df = read_input_file_into_df(new_path.clone());
    fs::remove_file(new_path).expect("could not delete file");
    return df;
}

pub fn create_user_df(
    folders: &Vec<PathBuf>,
    output_type: OutputType,
    date: Option<NaiveDate>,
) -> PolarsResult<DataFrame> {
    create_user_df_from_files(
        folders
            .iter()
            .map(|x| {
                let mut p = PathBuf::from(x);
                p.push(output_type.subdir());
                list_files(p as PathBuf).into_iter().map(|x| x.path())
            })
            .flatten()
            .collect(),
        output_type,
        date,
    )
}

fn flatten_df(df: DataFrame) -> Result<DataFrame, PolarsError> {
    let mut lazyframe = df.lazy();
    let left: Vec<String> = (1..=18).into_iter().map(|x| format!("l{}", x)).collect();
    let right: Vec<String> = (1..=18).into_iter().map(|x| format!("r{}", x)).collect();
    let bend: Vec<String> = (1..=18)
        .into_iter()
        .map(|x| format!("bend_{}", x))
        .collect();
    let twist: Vec<String> = (1..=18)
        .into_iter()
        .map(|x| format!("twist_{}", x))
        .collect();
    let acc: Vec<String> = ('x'..='z').into_iter().map(|x| x.to_string()).collect();
    let gyro: Vec<String> = vec!["ɑ", "β", "ɣ"]
        .into_iter()
        .map(|x| x.to_string())
        .collect();

    let column_names: Vec<&str> = vec!["left", "right", "acc", "gyro", "alpha", "beta"];

    for pair in column_names
        .clone()
        .into_iter()
        .zip(vec![left, right, acc, gyro, bend, twist])
        .into_iter()
    {
        for (index, ch) in pair.1.iter().enumerate().take(pair.1.len()) {
            lazyframe = lazyframe.with_columns([col(pair.0)
                .arr()
                .get(lit(index as i64))
                .alias(&ch.to_string())])
        }
    }

    let lazyframe = lazyframe.drop_columns(column_names);
    let lazyframe = lazyframe.drop_columns(["coords"]);

    lazyframe.collect()
}

fn write_flat_df(path: &PathBuf, df: DataFrame) {
    match flatten_df(df) {
        Ok(df) => {
            let file = &mut File::create(path).expect("could not create file");
            match CsvWriter::new(file)
                .has_header(false)
                .finish(&mut df.clone())
            {
                Ok(_) => println!("wrote file to {:?}", path),
                Err(e) => {
                    println!("could no write df 1 {e}")
                }
            }
        }
        Err(e) => println!("could no write df 2 {e}"),
    }
}

pub fn write_df(path: &PathBuf, df: &mut DataFrame) {
    let file = &mut File::create(path).expect("could not create file");
    match TableFormat::from_str(path.to_str().unwrap()) {
        Ok(e) => match e {
            TableFormat::Arrow => todo!("the arrow format writer is not yet implemented"),
            TableFormat::Csv => {
                if let Some(mut df) = Some(df.clone()) {
                    match CsvWriter::new(file).has_header(false).finish(&mut df) {
                        Ok(_) => println!("wrote file to {:?}", path),
                        _ => write_flat_df(path, df),
                    }
                } else {
                    match CsvWriter::new(file).has_header(false).finish(df) {
                        Ok(_) => println!("wrote file to {:?}", path),
                        Err(e) => {
                            println!("could no write df {e}")
                        }
                    }
                }
            }
            TableFormat::Parquet => {
                let mut df = if (!is_new_schema(&df))
                    && match infer_df_type(&df) {
                        OutputType::raw => true,
                        _ => false,
                    } {
                    transform_to_new_schema(df).unwrap()
                } else {
                    match infer_df_type(&df) {
                        OutputType::points | OutputType::logs => df.clone(),
                        OutputType::raw => match convert_i64_to_time(df, None) {
                            Ok(df) => df.clone(),
                            Err(_) => df.clone(),
                        },
                    }
                };
                match ParquetWriter::new(file)
                    .with_statistics(true)
                    .finish(&mut df)
                {
                    Ok(_) => println!("wrote df {:?}\n file to {:?}", df, path),
                    Err(_) => println!("failed to write file"),
                }
            }
        },
        Err(_) => todo!(),
    }
}

pub fn convert_time_to_i64(df: &mut DataFrame, column: Option<&str>) -> Option<DataFrame> {
    // TODO the polars parser doesn't recognize iso 8601 while parsing
    // therefore the time strings are converted back to i64, which is stupid
    // but otherwise the csv can't be parsed again
    if let Ok(col) = df.column(column.unwrap_or("t")) {
        if let Ok(col) = col.cast(&DataType::Int64) {
            return df.clone().with_column(col).ok().cloned();
        }
    }
    None
}

pub fn convert_i64_to_time(
    df: &mut DataFrame,
    time_unit: Option<TimeUnit>,
) -> PolarsResult<DataFrame> {
    if let Err(_) = df.column("t").unwrap().i64() {
        return Ok(df.clone());
    }

    let mut df = df.clone().filter(&ChunkedArray::from_iter(
        df.column("t")
            .unwrap()
            .i64()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap() > 0),
    ))?;
    Ok(df
        .with_column(df.column("t")?.cast(&DataType::Datetime(
            time_unit.unwrap_or(polars::prelude::TimeUnit::Milliseconds),
            Some("Europe/Berlin".into()),
        ))?)?
        .clone())
}

pub fn read_points_csv(path: &PathBuf) -> PolarsResult<DataFrame> {
    convert_i64_to_time(
        &mut CsvReader::from_path(path)?
            .with_schema(Arc::new(generate_points_schema()))
            .with_ignore_errors(true)
            .has_header(false)
            .finish()?,
        None,
    )
}

pub fn read_logs_csv(path: &PathBuf) -> PolarsResult<DataFrame> {
    let df = CsvReader::from_path(path)?
        .with_ignore_errors(true)
        .with_schema(Arc::new(OutputType::logs.schema(None).unwrap()))
        .has_header(false)
        .finish()?;

    // ass
    let mut df = df.filter(
        &df.column("t")
            .unwrap()
            .utf8()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap_or("").len() > 0 && parse_dart_timestring_short(x.unwrap()).is_ok())
            .collect(),
    )?;

    let s = DatetimeChunked::from_naive_datetime(
        "t",
        df.column("t")
            .unwrap()
            .utf8()
            .unwrap()
            .into_iter()
            .map(|x| parse_dart_timestring_short(x.unwrap()).unwrap()),
        TimeUnit::Milliseconds,
    );

    df.replace_or_add("t", s).cloned()
}

pub fn read_raw_csv(path: &PathBuf) -> Result<DataFrame, PolarsError> {
    let schema = Some(generate_flextail_schema(get_num_of_sensors_from_file(
        &path,
    )));
    let reader = CsvReader::from_path(path).unwrap().with_ignore_errors(true);

    let reader = match schema {
        Some(schema) => reader.with_schema(Arc::new(schema)),
        None => reader.infer_schema(Some(100)),
    };

    convert_i64_to_time(
        reader
            .has_header(false)
            .finish()
            .as_mut()
            .map_err(|_| PolarsError::NoData("cannot get as mut".into()))?,
        None,
    )
}

fn read_csv_file(file: &PathBuf, output_type: OutputType) -> PolarsResult<DataFrame> {
    (match output_type {
        OutputType::points => read_points_csv,
        OutputType::raw => read_raw_csv,
        OutputType::logs => read_logs_csv,
    })(file)
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

pub enum SusLevel {
    Ok,
    Sus(String),
    TurboSus(String),
}

fn validate_rows(df: DataFrame) -> SusLevel {
    let n = (df.shape().1 - 8) / 2;
    let mut sus_counter: usize = 0;

    for i in 0..df.shape().0 {
        if any_value_to_i16(
            df.get_row(i)
                .unwrap()
                .0
                .iter()
                .take(2 * n)
                .collect::<Vec<&AnyValue<'_>>>(),
        )
        .into_iter()
        .filter(|x| x.abs() > 500)
        .count()
            > 2
        {
            sus_counter += 1;
        }
    }

    let sus_percent = sus_counter as f32 / df.shape().0 as f32;
    if sus_percent > 0.02 {
        SusLevel::TurboSus(format!("{}% faulty rows", (100.0 * sus_percent).round()))
    } else if sus_percent > 0.01 {
        SusLevel::Sus(format!("{}% faulty rows", (100.0 * sus_percent).round()))
    } else {
        SusLevel::Ok
    }
}

pub fn validate_file(path: &PathBuf) -> SusLevel {
    match read_raw_csv(path) {
        Ok(df) => {
            if df.is_empty() {
                return SusLevel::TurboSus("empty".to_string());
            } else {
                validate_rows(df)
            }
        }
        _ => SusLevel::TurboSus("could not be parsed".to_string()),
    }
}
