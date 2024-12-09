// use crate::DataFrame;
// use std::error::Error;

// pub fn calculate_signal(df: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
//     let mut signal_data = Vec::with_capacity(df.data.len());

//     for row in &df.data {
//         let signal_row: Vec<f64> = row.iter()
//             .map(|&x| if x > 0.0 { 1.0 } else { -1.0 })
//             .collect();
//         signal_data.push(signal_row);
//     }

//     Ok(DataFrame {
//         dates: df.dates.clone(),
//         data: signal_data,
//         column_names: df.column_names.clone(),
//     })
// }

// pub fn main(df: &DataFrame) -> Result<DataFrame, Box<dyn Error>> {
//     calculate_signal(df)
// }
