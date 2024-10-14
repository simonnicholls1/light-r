use light_r::{DataFrame, operations::load};
use chrono::NaiveDate;
use tempfile::NamedTempFile;
use std::io::Write;

#[test]
fn test_load() {
    let csv_content = "DATE,Column1,Column2
2021-01-01,1.0,2.0
2021-01-02,3.0,4.0
2021-01-03,5.0,6.0
";

    let temp_file = NamedTempFile::new().unwrap();
    write!(temp_file.as_file_mut(), "{}", csv_content).unwrap();

    let result = load::load(temp_file.path().to_str().unwrap()).unwrap();
    
    assert_eq!(result.dates.len(), 3);
    assert_eq!(result.data.len(), 3);
    assert_eq!(result.column_names, vec!["Column1", "Column2"]);

    assert_eq!(result.dates[0], NaiveDate::from_ymd_opt(2021, 1, 1).unwrap());
    assert_eq!(result.data[0], vec![1.0, 2.0]);

    assert_eq!(result.dates[1], NaiveDate::from_ymd_opt(2021, 1, 2).unwrap());
    assert_eq!(result.data[1], vec![3.0, 4.0]);

    assert_eq!(result.dates[2], NaiveDate::from_ymd_opt(2021, 1, 3).unwrap());
    assert_eq!(result.data[2], vec![5.0, 6.0]);
}

#[test]
fn test_load_with_missing_values() {
    let csv_content = "DATE,Column1,Column2
2021-01-01,1.0,
2021-01-02,,4.0
2021-01-03,5.0,6.0
";

    let temp_file = NamedTempFile::new().unwrap();
    write!(temp_file.as_file_mut(), "{}", csv_content).unwrap();

    let result = load::load(temp_file.path().to_str().unwrap()).unwrap();
    
    assert_eq!(result.dates.len(), 3);
    assert_eq!(result.data.len(), 3);

    assert!(result.data[0][1].is_nan());
    assert!(result.data[1][0].is_nan());
}

#[test]
fn test_main() {
    let csv_content = "DATE,Column1\n2021-01-01,1.0\n";
    let temp_file = NamedTempFile::new().unwrap();
    write!(temp_file.as_file_mut(), "{}", csv_content).unwrap();

    let result = load::main(temp_file.path().to_str().unwrap()).unwrap();
    assert_eq!(result.dates.len(), 1);
    assert_eq!(result.data.len(), 1);
    assert_eq!(result.column_names, vec!["Column1"]);
}
