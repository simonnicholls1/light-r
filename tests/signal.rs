use light_r::{DataFrame, operations::signal};
use chrono::NaiveDate;

#[test]
fn test_calculate_signal() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
    ];
    let data = vec![
        vec![1.0, -2.0, 0.0],
        vec![-3.0, 4.0, 0.5],
        vec![0.1, -0.1, 0.0],
    ];
    let column_names = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = signal::calculate_signal(&df).unwrap();
    
    assert_eq!(result.data[0], vec![1.0, -1.0, -1.0]);
    assert_eq!(result.data[1], vec![-1.0, 1.0, 1.0]);
    assert_eq!(result.data[2], vec![1.0, -1.0, -1.0]);
}

#[test]
fn test_calculate_signal_all_positive() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data = vec![vec![1.0, 2.0, 3.0]];
    let column_names = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = signal::calculate_signal(&df).unwrap();
    
    assert_eq!(result.data[0], vec![1.0, 1.0, 1.0]);
}

#[test]
fn test_calculate_signal_all_negative() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data = vec![vec![-1.0, -2.0, -3.0]];
    let column_names = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = signal::calculate_signal(&df).unwrap();
    
    assert_eq!(result.data[0], vec![-1.0, -1.0, -1.0]);
}

#[test]
fn test_main() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data = vec![vec![1.0, -1.0, 0.0]];
    let column_names = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = signal::main(&df).unwrap();
    assert_eq!(result.data[0], vec![1.0, -1.0, -1.0]);
}
