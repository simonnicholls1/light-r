// use crate::DataFrame;
// use std::error::Error;
// use std::fs::File;
// use std::io::Write;

// pub fn save(df: &DataFrame, filename: &str) -> Result<(), Box<dyn Error>> {
//     let mut file = File::create(filename)?;

//     // Write header
//     writeln!(file, "DATE,{}", df.column_names.join(","))?;

//     // Write data
//     for (date, row) in df.dates.iter().zip(df.data.iter()) {
//         write!(file, "{}", date.format("%Y-%m-%d"))?;
//         for value in row {
//             write!(file, ",{}", value)?;
//         }
//         writeln!(file)?;
//     }

//     Ok(())
// }

// pub fn main(df: &DataFrame, filename: &str) -> Result<DataFrame, Box<dyn Error>> {
//     save(df, filename)?;
//     Ok(df.clone())
// }
