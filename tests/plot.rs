use light_r::{DataFrame, operations::plot};
use chrono::NaiveDate;
use std::fs;

#[test]
fn test_plot() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0],
        vec![2.0, 3.0],
        vec![3.0, 4.0],
    ];
    let column_names = vec!["A".to_string(), "B".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = plot::plot(&df);
    assert!(result.is_ok());

    // Check if the file was created
    assert!(fs::metadata("plot.html").is_ok());

    // Clean up
    fs::remove_file("plot.html").unwrap();
}

#[test]
fn test_main() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data = vec![vec![1.0]];
    let column_names = vec!["A".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = plot::main(&df);
    assert!(result.is_ok());

    // Check if the file was created
    assert!(fs::metadata("plot.html").is_ok());

    // Clean up
    fs::remove_file("plot.html").unwrap();
}
