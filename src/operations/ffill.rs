// use crate::DataFrame;

// pub fn ffill(df: &DataFrame) -> DataFrame {
//     let mut result = DataFrame {
//         dates: df.dates.clone(),
//         data: vec![vec![f64::NAN; df.data[0].len()]; df.data.len()],
//         column_names: df.column_names.clone()
//     };

//     for col in 0..df.data[0].len() {
//         let mut last_valid = f64::NAN;
//         for row in 0..df.data.len() {
//             if !df.data[row][col].is_nan() {
//                 last_valid = df.data[row][col];
//             }
//             result.data[row][col] = last_valid;
//         }
//     }

//     result
// }

// pub fn main(df: &DataFrame) -> DataFrame {
//     ffill(df)
// }
