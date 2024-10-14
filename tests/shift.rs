use light_r::{DataFrame, operations::shift};
use chrono::NaiveDate;
use approx::assert_relative_eq;

#[test]
fn test_shift_positive() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0],
        vec![3.0, 4.0],
        vec![5.0, 6.0],
    ];
    let column_names = vec!["A".to_string(), "B".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = shift::shift(&df, 1).unwrap();
    
    assert!(result.data[0][0].is_nan());
    assert!(result.data[0][1].is_nan());
    assert_relative_eq!(result.data[1][0], 1.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][1], 2.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[2][0], 3.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[2][1], 4.0, epsilon = 1e-6);
}

#[test]
fn test_shift_negative() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0],
        vec![3.0, 4.0],
        vec![5.0, 6.0],
    ];
    let column_names = vec!["A".to_string(), "B".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = shift::shift(&df, -1).unwrap();
    
    assert_relative_eq!(result.data[0][0], 3.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[0][1], 4.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][0], 5.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][1], 6.0, epsilon = 1e-6);
    assert!(result.data[2][0].is_nan());
    assert!(result.data[2][1].is_nan());
}

#[test]
fn test_shift_zero() {
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

    let result = shift::shift(&df, 0).unwrap();
    
    assert_relative_eq!(result.data[0][0], 1.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[0][1], 2.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][0], 3.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][1], 4.0, epsilon = 1e-6);
}

#[test]
fn test_main() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data = vec![
        vec![1.0],
        vec![2.0],
    ];
    let column_names = vec!["A".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = shift::main(&df, 1).unwrap();
    assert!(result.data[0][0].is_nan());
    assert_relative_eq!(result.data[1][0], 1.0, epsilon = 1e-6);
}
