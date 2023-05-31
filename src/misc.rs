use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    time::SystemTime,
};

use chrono::NaiveDateTime;
use polars::prelude::DataFrame;

use crate::schema::OutputType;

pub fn read_first_line(path: &PathBuf) -> String {
    let f = std::fs::File::open(path).unwrap();
    let mut buf = String::new();
    BufReader::new(f)
        .read_line(&mut buf)
        .expect("could not read first line");
    buf
}

pub fn get_number_of_csv_fields(path: &PathBuf) -> usize {
    let f = std::fs::File::open(path).unwrap();
    let mut buf = String::new();
    BufReader::new(f)
        .read_line(&mut buf)
        .expect("could not read first line");
    buf.splitn(100, ',').count()
}

pub fn get_num_of_sensors_from_file(dir: &PathBuf) -> usize {
    get_num_of_sensors(get_number_of_csv_fields(dir))
}

pub fn get_num_of_sensors(num_of_fields: usize) -> usize {
    if num_of_fields >= 7 {
        (num_of_fields - 7) / 2
    } else {
        0
    }
}

pub fn infer_file_type(path: &PathBuf) -> OutputType {
    let n = get_number_of_csv_fields(path);
    match n {
        5 => OutputType::points,
        10.. => OutputType::raw,
        _ => OutputType::logs,
    }
}

pub fn infer_df_type(df: &DataFrame) -> OutputType {
    let schema = df.schema();
    if schema.contains("v") {
        OutputType::raw
    } else if schema.contains("score") {
        OutputType::points
    } else {
        OutputType::logs
    }
}

pub fn is_new_schema(df: &DataFrame) -> bool {
    df.schema().contains("left")
}

pub fn timeit<F: Fn() -> T, T>(f: F) -> T {
    let start = SystemTime::now();
    let result = f();
    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!("took {} ms", duration.as_millis());
    result
}

pub fn parse_dart_timestring(s: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(s.split_once(".").unwrap().0, "%Y-%m-%d %H:%M:%S")
}

pub fn parse_dart_timestring_short(s: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S.%f")
}
