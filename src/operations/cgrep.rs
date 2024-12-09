// use crate::DataFrame;

// pub fn filter_columns(df: &DataFrame, column_names: &[String]) -> DataFrame {
//     let selected_indices: Vec<usize> = column_names
//         .iter()
//         .filter_map(|name| df.column_names.iter().position(|n| n == name))
//         .collect();

//     DataFrame {
//         dates: df.dates.clone(),
//         data: df.data.iter()
//             .map(|row| selected_indices.iter().map(|&i| row[i]).collect())
//             .collect(),
//         column_names: selected_indices.iter().map(|&i| df.column_names[i].clone()).collect(),
//     }
// }

// pub fn main(df: &DataFrame, columns: &[String]) -> Result<DataFrame, Box<dyn std::error::Error>> {
//     if df.data.is_empty() {
//         return Err("No data available".into());
//     }
//     Ok(filter_columns(df, columns))
// }
