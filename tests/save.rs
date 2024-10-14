use light_r::{DataFrame, operations::save};
use chrono::NaiveDate;
use std::fs;
use std::io::Read;

#[test]
fn test_save() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0],
        vec![3.0, 4.0],
    ];
    let column_names = vec!["A".to_string(), "B".to_string()];
    let df = DataFrame { dates, data, column_names };

    let filename = "test_save.csv";
    let result = save::save(&df, filename);
    assert!(result.is_ok());

    // Read the contents of the file
    let mut file = fs::File::open(filename).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    // Check the contents
    let expected_contents = "DATE,A,B\n2021-01-01,1,2\n2021-01-02,3,4\n";
    assert_eq!(contents, expected_contents);

    // Clean up
    fs::remove_file(filename).unwrap();
}

#[test]
fn test_main() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data = vec![vec![1.0]];
    let column_names = vec!["A".to_string()];
    let df = DataFrame { dates, data, column_names };

    let filename = "test_main.csv";
    let result = save::main(&df, filename);
    assert!(result.is_ok());

    // Check if the file was created
    assert!(fs::metadata(filename).is_ok());

    // Clean up
    fs::remove_file(filename).unwrap();
}
