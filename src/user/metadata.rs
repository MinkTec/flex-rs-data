use super::daily_activities::DailyActivities;
use crate::{
    feedback::{BackpainFeedback, RectifyFeedback},
    fs::{AppVersion, PhoneModel},
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use timespan::TimedData;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMetadata {
    pub sensors: HashSet<String>,
    pub initial_app_start: Option<NaiveDateTime>,
    pub number_of_measured_days: Option<usize>,
    pub average_score: Option<f32>,
    pub phone: Option<PhoneModel>,
    pub app_version: Option<AppVersion>,
    pub activities: Option<DailyActivities>,
    pub app_feedback: Vec<TimedData<RectifyFeedback>>,
    pub backpain_feedback: Vec<TimedData<BackpainFeedback>>,
}

impl UserMetadata {
    pub fn new() -> UserMetadata {
        UserMetadata {
            sensors: HashSet::new(),
            initial_app_start: None,
            number_of_measured_days: None,
            average_score: None,
            phone: None,
            app_version: None,
            activities: None,
            app_feedback: vec![],
            backpain_feedback: vec![],
        }
    }

    pub fn with_sensors(mut self, sensors: HashSet<String>) -> UserMetadata {
        self.sensors = sensors;
        self
    }

    pub fn with_active_since(mut self, active_since: Option<NaiveDateTime>) -> UserMetadata {
        self.initial_app_start = active_since;
        self
    }

    pub fn with_number_of_measured_days(mut self, d: Option<usize>) -> UserMetadata {
        self.number_of_measured_days = d;
        self
    }

    pub fn with_daily_activities(mut self, activites: DailyActivities) -> UserMetadata {
        self.activities = Some(activites);
        self
    }
}
