use chrono::NaiveDate;

#[derive(Clone)]
pub struct DataFrame {
    pub dates: Vec<NaiveDate>,
    pub data: Vec<Vec<f64>>,
    pub column_names: Vec<String>,
}
