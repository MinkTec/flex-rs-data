use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    str::FromStr,
};

use enum_iterator::{all, Sequence};
use serde::{Deserialize, Serialize};
use serde_json::Number;

#[allow(
    dead_code,
    non_snake_case,
    non_upper_case_globals,
    enum_intrinsics_non_enums,
    non_camel_case_types
)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RectifyFeedback {
    shirtComfort: ShirtComfort,
    sensorIsMoving: SensorMovement,
    shirtWearLocations: HashMap<String, bool>,
    shirtWearDuration: ShirtWearDuration,
    shirtWearWeekly: ShirtWearWeekly,
    rectifyDuration: RectifyDuration,

    // Die App
    appUsability: AppUsability,
    rectifyBenefit: RectifyBenefit,

    vibrationBenefit: Number,
    saturationBenefit: Number,
    evaluationBenefit: Number,
    miniExerciseBenefit: Number,
    trainingBenefit: Number,
    pub otherFeatureWishes: Option<String>,

    // Vibrationsalarm
    vibrationLevelPref: Number,
    vibrationMissingWhen: Option<String>,
    vibrationIs: VibrationIsValue,
    pub otherWishes: Option<String>,

    // Score
    reductionWhileSitting: SpeedOptions,
    increaseWhileMoving: String,

    occuredBugs: OccuredBugs,
    buyRectify: BuyRectify,
    rectifyPrice: Number,
    rectifyPricespan: Option<String>,
    pub eMail: Option<String>,
}

impl FromStr for RectifyFeedback {
    type Err = FeedbackParseError;

    fn from_str(s: &str) -> Result<Self, FeedbackParseError> {
        serde_json::from_str::<RectifyFeedback>(s).map_err(|e| {
            println!("parsing error: {}", e);
            FeedbackParseError
        })
    }
}

impl Display for RectifyFeedback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"shirtComfort: {:?},
sensorIsMoving: {:?},
shirtWearLocations: {},
shirtWearDuration: {:?},
shirtWearWeekly: {:?},
rectifyDuration: {:?},

// Die App
appUsability: {:?},
rectifyBenefit: {:?},

vibrationBenefit: {:?},
saturationBenefit: {:?},
evaluationBenefit: {:?},
miniExerciseBenefit: {:?},
trainingBenefit: {:?},
otherFeatureWishes: {},

// Vibrationsalarm
vibrationLevelPref: {},
vibrationMissingWhen: {:?},
vibrationIs: {:?},
otherWishes: {:?},

// Score
reductionWhileSitting: {:?},
increaseWhileMoving: {},

occuredBugs: {:?},
buyRectify: {:?},
rectifyPrice: {},
rectifyPricespan: {:?},
eMail: {:?}"#,
            self.shirtComfort,
            self.sensorIsMoving,
            self.shirtWearLocations.print(),
            self.shirtWearDuration,
            self.shirtWearWeekly,
            self.rectifyDuration,
            self.appUsability,
            self.rectifyBenefit,
            self.vibrationBenefit.print(),
            self.saturationBenefit.print(),
            self.evaluationBenefit.print(),
            self.miniExerciseBenefit.print(),
            self.trainingBenefit.print(),
            self.otherFeatureWishes.print(),
            self.vibrationLevelPref.print(),
            self.vibrationMissingWhen.print(),
            self.vibrationIs,
            self.otherWishes.print(),
            self.reductionWhileSitting,
            self.increaseWhileMoving,
            self.occuredBugs,
            self.buyRectify,
            self.rectifyPrice,
            self.rectifyPricespan,
            self.eMail
        )
    }
}

#[allow(non_snake_case, non_camel_case_types)]
#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
enum VibrationIsValue {
    ist_gut_Genug,
    vibriert_unerwartet,
    vibriert_nicht_wenn_ich_es_erwarte,
    weak,
    dontUnderstand,
    sonstiges,
    na,
}

#[allow(non_snake_case, non_camel_case_types)]
#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
enum SpeedOptions {
    toSlow,
    good,
    toFast,
    na,
}

#[allow(non_snake_case, non_camel_case_types)]
#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum MotivationOptions {
    Very,
    Somewhat,
    Not_really,
    Not,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum ShirtComfort {
    Comfy,
    Ok,
    Uncomfy,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum SensorMovement {
    Annoying,
    Sometimes,
    Good,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum ShirtWearLocation {
    Work(Option<bool>),
    Sparetime(Option<bool>),
    Sport(Option<bool>),
    Sleep(Option<bool>),
    Other(Option<bool>),
    Na(Option<bool>),
}

#[allow(non_snake_case, non_camel_case_types)]
#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
enum AppUsability {
    intelligible,
    slightlyComplicated,
    complicated,
    na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum OccuredBugs {
    No,
    Some,
    Alot,
    Tomuch,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum ShirtWearDuration {
    Four,
    Foureight,
    Eighttwelve,
    Day,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum ShirtWearWeekly {
    One,
    Two,
    Three,
    Seven,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum RectifyDuration {
    Four,
    Eight,
}

#[allow(non_snake_case, non_camel_case_types)]
#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
enum RectifyBenefit {
    veryUseful,
    useful,
    notReallyUseful,
    notUseful,
    na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum BuyRectify {
    Yes,
    No,
    Na,
}

#[allow(
    dead_code,
    non_snake_case,
    non_upper_case_globals,
    enum_intrinsics_non_enums,
    non_camel_case_types
)]
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BackpainFeedback {
    gender: Gender,
    age: Number,
    weight: Number,
    bodyHeight: Number,

    backpainFrequency: BackpainLevel,
    ifBackpainWhere: HashMap<IfBackpainWhere, bool>,
    ifBackpainWhereLR: HashMap<IfBackpainWhere, Number>,
    backpainLevel: Number,
    walkingPain: HashMap<WalkingPain, bool>,
    walkingPainLevel: Number,
    painProblems: PainProblems,

    // Selbstwahrnehmung
    postureSelf: Number,
    mobilifySelf: Number,
    movementAtWork: Number,
    movementFreeTime: Number,

    standingDesk: StandingDesk,
    sittingStandingSwitch: SittingStandingSwitch,

    // legacy
    heavyObject: AutonomyLevel,
    highObject: AutonomyLevel,
    knowAboutFitForWork: YesNo,
    longStanding: AutonomyLevel,
    lowObject: AutonomyLevel,
    motivatedForFitWork: Option<bool>,
    takePartInFitForWork: Option<bool>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct FeedbackParseError;

impl FromStr for BackpainFeedback {
    type Err = FeedbackParseError;

    fn from_str(s: &str) -> Result<Self, FeedbackParseError> {
        YesNo::Yes.number();
        YesNo::No.number();
        YesNo::Na.number();

        serde_json::from_str::<BackpainFeedback>(s).map_err(|e| {
            println!("{}", e);
            FeedbackParseError
        })
    }
}

impl Display for BackpainFeedback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"gender: {:?},
age: {},
weight: {},
bodyHeight: {},

backpainFrequency: {:?},
ifBackpainWhere:
{},
ifBackpainWhereLR:
{},
backpainLevel: {:?},
walkingPain:
{},
walkingPainLevel: {},
painProblems: {:?},

// Selbstwahrnehmung
postureSelf: {},
mobilifySelf: {},
movementAtWork: {},
movementFreeTime: {},

standingDesk: {:?},
sittingStandingSwitch: {:?}"#,
            self.gender,
            match self.age.as_u64() {
                Some(i) => AGE_RANGE[i as usize],
                None => "-",
            },
            match self.weight.as_u64() {
                Some(i) => WEIGTH[i as usize],
                None => "-",
            },
            match self.bodyHeight.as_u64() {
                Some(i) => HEIGHT[i as usize],
                None => "-",
            },
            self.backpainFrequency,
            self.ifBackpainWhere.print(),
            self.ifBackpainWhereLR.print(),
            self.backpainLevel.print(),
            self.walkingPain.print(),
            self.walkingPainLevel.print(),
            self.painProblems,
            self.postureSelf.print(),
            self.mobilifySelf.print(),
            self.movementAtWork.print(),
            self.movementFreeTime.print(),
            self.standingDesk,
            self.sittingStandingSwitch,
        )
    }
}

trait CustomPrint {
    fn print(&self) -> String;
}

impl CustomPrint for Option<String> {
    fn print(&self) -> String {
        self.clone().unwrap_or(String::from("-"))
    }
}

impl CustomPrint for HashMap<String, bool> {
    fn print(&self) -> String {
        self.iter()
            .map(|(k, v)| format!("\t{}: {:?}", k, v))
            .collect::<Vec<String>>()
            .join("\n")
    }
}

impl CustomPrint for HashMap<IfBackpainWhere, bool> {
    fn print(&self) -> String {
        self.iter()
            .map(|(k, v)| format!("\t{:?}: {:?}", k, v))
            .collect::<Vec<String>>()
            .join("\n")
    }
}

impl CustomPrint for HashMap<IfBackpainWhere, Number> {
    fn print(&self) -> String {
        self.iter()
            .map(|(k, v)| format!("\t{:?}: {:?}", k, LeftRightRange::from(v.clone())))
            .collect::<Vec<String>>()
            .join("\n")
    }
}

impl CustomPrint for HashMap<WalkingPain, bool> {
    fn print(&self) -> String {
        self.iter()
            .map(|(k, v)| format!("\t{:?}: {:?}", k, v))
            .collect::<Vec<String>>()
            .join("\n")
    }
}

impl CustomPrint for Number {
    fn print(&self) -> String {
        match self.as_i64() {
            Some(i) => i.to_string(),
            _ => match self.as_f64() {
                Some(i) => i.to_string(),
                None => String::from("-"),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Gender {
    Male,
    Female,
    Diverse,
    Undefined,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum BackpainLevel {
    No,
    Seldom,
    Sometimes,
    Often,
    Always,
    Recently,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
enum LeftRightRange {
    OnlyLeft,
    MostlyLeft,
    Center,
    MostlyRight,
    OnlyRight,
    Na,
}

impl From<Number> for LeftRightRange {
    fn from(value: Number) -> Self {
        match value.as_i64() {
            Some(v) => match v {
                -2 => LeftRightRange::OnlyLeft,
                -1 => LeftRightRange::MostlyLeft,
                -0 => LeftRightRange::Center,
                1 => LeftRightRange::MostlyRight,
                2 => LeftRightRange::OnlyRight,
                _ => LeftRightRange::Na,
            },
            None => LeftRightRange::Na,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Hash, PartialEq, Eq, Clone)]
#[serde(rename_all = "lowercase")]
enum IfBackpainWhere {
    Cervical,
    Thorax,
    Lumbal,
    Hip,
    Na,
}

#[allow(non_snake_case, non_camel_case_types)]
#[derive(Debug, Deserialize, Serialize, Hash, PartialEq, Eq, Clone)]
enum WalkingPain {
    no,
    back,
    hip,
    rightKnee,
    leftKnee,
    rightFoot,
    leftFoot,
    yes,
    na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum PainProblems {
    No,
    Some,
    Medium,
    More,
    Very,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum AutonomyLevel {
    Yes,
    Difficult,
    No,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum SittingStandingSwitch {
    Never,
    Once,
    Twice,
    Hourly,
    Often,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum StandingDesk {
    Yes,
    No,
    Nodesk,
    Na,
}

#[derive(Debug, Deserialize, Serialize, Sequence, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum YesNo {
    Yes,
    No,
    Na,
}

trait Numbering {
    fn number(&self) -> i8;
}

impl<T> Numbering for T
where
    T: Serialize + Sequence + PartialEq + Copy,
{
    fn number(&self) -> i8 {
        let (i, val) = all::<T>()
            .enumerate()
            .find(|a| a.1 == self.clone())
            .unwrap();

        if serde_json::to_string(&val)
            .unwrap()
            .to_lowercase()
            .contains("na")
        {
            -1
        } else {
            i as i8
        }
    }
}

const AGE_RANGE: &[&str] = &[
    "< 10 years",
    "10 - 14 years",
    "15 - 19 years",
    "20 - 24 years",
    "25 - 29 years",
    "30 - 34 years",
    "35 - 39 years",
    "40 - 44 years",
    "45 - 49 years",
    "50 - 54 years",
    "55 - 59 years",
    "60 - 64 years",
    "65 - 69 years",
    "70 - 74 years",
    "75 - 79 years",
    "80 - 84 years",
    "85 - 90 years",
    "> 90 years",
];

const WEIGTH: &[&str] = &[
    "",
    "",
    "< 30",
    "30 - 39 kg",
    "40 - 49 kg",
    "50 - 59 kg",
    "60 - 69 kg",
    "70 - 79 kg",
    "80 - 89 kg",
    "90 - 99 kg",
    "100 - 109 kg",
    "110 - 120 kg",
    "> 120 kg",
];

const HEIGHT: &[&str] = &[
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "< 1 m",
    "100 - 109 cm",
    "110 - 119 cm",
    "120 - 129 cm",
    "130 - 139 cm",
    "140 - 149 cm",
    "150 - 159 cm",
    "160 - 169 cm",
    "170 - 179 cm",
    "180 - 189 cm",
    "190 - 200 cm",
    "> 200 cm",
];

pub fn parse_feedback(feedback: &str) {
    match serde_json::from_str::<BackpainFeedback>(feedback) {
        Ok(res) => {
            println!("{}", res);
        }
        _ => match serde_json::from_str::<RectifyFeedback>(feedback) {
            Ok(res) => {
                println!("{:?}", res);
            }
            Err(e) => {
                println!("json parse error: {}", e);
                println!("{}", feedback);
            }
        },
    }
}

pub struct FeedbackCsv {}

impl FeedbackCsv {
    pub fn get_header() -> String {
        "shirtComfort, sensorIsMoving, shirtWearLocations, shirtWearDuration, shirtWearWeekly, rectifyDuration, appUsability, rectifyBenefit, vibrationBenefit, saturationBenefit, evaluationBenefit, miniExerciseBenefit, trainingBenefit, otherFeatureWishes, vibrationLevelPref, vibrationMissingWhen, vibrationIs, otherWishes, reductionWhileSitting, increaseWhileMoving, occuredBugs, buyRectify, rectifyPrice, rectifyPricespan, gender, age, weight, bodyHeight, backpainFrequency, ifBackpainWhere, ifBackpainWhereLR, backpainLevel, walkingPain, walkingPainLevel, painProblems, postureSelf, mobilifySelf, movementAtWork, movementFreeTime, standingDesk, sittingStandingSwitch".to_string()
    }
}

pub fn gen_csv_line(rectify: RectifyFeedback, backpain: BackpainFeedback) -> String {
    format!(
        r#"{:?}, {:?}, {}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {}, {:?}, {:?}, {:?}, {:?}, {}, {:?}, {:?}, {}, {:?}, {:?}, {:?}, {}, {}, {}, {:?}, {}, {}, {:?}, {}, {}, {:?}, {}, {}, {}, {}, {:?}, {:?}"#,
        rectify.shirtComfort,
        rectify.sensorIsMoving,
        rectify.shirtWearLocations.print(),
        rectify.shirtWearDuration,
        rectify.shirtWearWeekly,
        rectify.rectifyDuration,
        rectify.appUsability,
        rectify.rectifyBenefit,
        rectify.vibrationBenefit.print(),
        rectify.saturationBenefit.print(),
        rectify.evaluationBenefit.print(),
        rectify.miniExerciseBenefit.print(),
        rectify.trainingBenefit.print(),
        rectify.otherFeatureWishes.print(),
        rectify.vibrationLevelPref.print(),
        rectify.vibrationMissingWhen.print(),
        rectify.vibrationIs,
        rectify.otherWishes.print(),
        rectify.reductionWhileSitting,
        rectify.increaseWhileMoving,
        rectify.occuredBugs,
        rectify.buyRectify,
        rectify.rectifyPrice,
        rectify.rectifyPricespan,
        rectify.eMail,
        backpain.gender,
        match backpain.age.as_u64() {
            Some(i) => AGE_RANGE[i as usize],
            None => "-",
        },
        match backpain.weight.as_u64() {
            Some(i) => WEIGTH[i as usize],
            None => "-",
        },
        match backpain.bodyHeight.as_u64() {
            Some(i) => HEIGHT[i as usize],
            None => "-",
        },
        backpain.backpainFrequency,
        backpain.ifBackpainWhere.print(),
        backpain.ifBackpainWhereLR.print(),
        backpain.backpainLevel.print(),
        backpain.walkingPain.print(),
        backpain.walkingPainLevel.print(),
        backpain.painProblems,
        backpain.postureSelf.print(),
        backpain.mobilifySelf.print(),
        backpain.movementAtWork.print(),
        backpain.movementFreeTime.print(),
        backpain.standingDesk,
        backpain.sittingStandingSwitch,
    ).replace("\n", " | ").replace("\t", " ").replace(r#"\""#, "")
}
