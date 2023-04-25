pub enum FeedbackType {
    Rectify,
    Backpain,
}

impl FeedbackType {
    pub fn matcher<'a>(self) -> &'a str {
        match self {
            FeedbackType::Rectify => "rectify_",
            FeedbackType::Backpain => "backpain_",
        }
    }
}
