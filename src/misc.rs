use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
};

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
    let n = get_number_of_csv_fields(dir);
    if n >= 7 {
        (n - 7) / 2
    } else {
        0
    }
}

pub fn infer_file_type(path: &PathBuf) -> OutputType {
    let n = get_number_of_csv_fields(path);
    println!("number of fields: {}", n);
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
