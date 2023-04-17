use std::{collections::HashSet, path::PathBuf};

use chrono::NaiveDate;
use serde::Serialize;

use crate::fs::{find_inital_app_start, find_uuid_folders, find_uuids_after, find_sensors};

#[derive(Debug, Clone, Serialize)]
pub struct User {
    id: String,
    sensor: HashSet<String>,
    active_since: Option<NaiveDate>,
    dirs: HashSet<PathBuf>,
}

pub fn gen_users(path: &PathBuf) -> Vec<User> {
    let uuids = find_uuids_after(path, &NaiveDate::from_ymd_opt(2023, 3, 1).unwrap());
    let users = uuids
        .into_iter()
        .map(|uuid| {
            let u = User::new(path, uuid);
            println!("{:?}", u);
            u
        })
        .collect();

    println!("users: {:?}", users);

    users
}

impl User {
    pub fn new(path: &PathBuf, uuid: String) -> User {
        let folders = find_uuid_folders(path, uuid.as_str());
        User {
            id: uuid,
            sensor: find_sensors(&folders),
            active_since: find_inital_app_start(&folders),
            dirs: folders,
        }
    }
}
