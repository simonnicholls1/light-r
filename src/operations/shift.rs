// use crate::DataFrame;
// use std::error::Error;

// pub fn shift(df: &DataFrame, shift_by: i32) -> Result<DataFrame, Box<dyn Error>> {
//     let mut shifted_data = Vec::with_capacity(df.data.len());
//     let num_columns = df.data[0].len();

//     if shift_by >= 0 {
//         // Shift down
//         for _ in 0..shift_by {
//             shifted_data.push(vec![f64::NAN; num_columns]);
//         }
//         shifted_data.extend_from_slice(&df.data[..(df.data.len() - shift_by as usize)]);
//     } else {
//         // Shift up
//         shifted_data.extend_from_slice(&df.data[(-shift_by as usize)..]);
//         for _ in 0..(-shift_by) {
//             shifted_data.push(vec![f64::NAN; num_columns]);
//         }
//     }

//     Ok(DataFrame {
//         dates: df.dates.clone(),
//         data: shifted_data,
//         column_names: df.column_names.clone(),
//     })
// }

// pub fn main(df: &DataFrame, shift_by: i32) -> Result<DataFrame, Box<dyn Error>> {
//     shift(df, shift_by)
// }
