// use crate::DataFrame;

// pub fn cumulative_sum(df: &mut DataFrame, start_number: f64) -> DataFrame {
//     // Remove rows with NaN values
//     df.data.retain(|row| row.iter().all(|&x| !x.is_nan()));
//     df.dates = df.dates[..df.data.len()].to_vec();

//     if !df.data.is_empty() {
//         for value in &mut df.data[0] {
//             *value += start_number;
//         }
//     }

//     let mut cumsum_data = Vec::with_capacity(df.data.len());
//     let mut running_sum = vec![0.0; df.data[0].len()];

//     for row in &df.data {
//         for (i, &value) in row.iter().enumerate() {
//             running_sum[i] += value;
//         }
//         cumsum_data.push(running_sum.clone());
//     }

//     DataFrame {
//         dates: df.dates.clone(),
//         data: cumsum_data,
//         column_names: df.column_names.clone()
//     }
// }

// pub fn main(df: &mut DataFrame, start_number: f64) -> Result<DataFrame, Box<dyn std::error::Error>> {
//     Ok(cumulative_sum(df, start_number))
// }
