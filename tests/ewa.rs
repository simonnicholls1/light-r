use light_r::{DataFrame, operations::ewa};
use chrono::NaiveDate;
use approx::assert_relative_eq;

#[test]
fn test_equally_weighted_average() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];
    let df = DataFrame { dates, data };

    let result = ewa::equally_weighted_average(&df).unwrap();
    
    assert_eq!(result.dates.len(), 3);
    assert_eq!(result.data.len(), 3);
    assert_relative_eq!(result.data[0][0], 2.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][0], 5.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[2][0], 8.0, epsilon = 1e-6);
}

#[test]
fn test_equally_weighted_average_with_nan() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0, f64::NAN],
        vec![4.0, f64::NAN, 6.0],
        vec![f64::NAN, f64::NAN, f64::NAN],
    ];
    let df = DataFrame { dates, data };

    let result = ewa::equally_weighted_average(&df).unwrap();
    
    assert_eq!(result.dates.len(), 2);
    assert_eq!(result.data.len(), 2);
    assert_relative_eq!(result.data[0][0], 1.5, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][0], 5.0, epsilon = 1e-6);
}

#[test]
fn test_equally_weighted_average_empty_data() {
    let df = DataFrame {
        dates: vec![],
        data: vec![],
    };
    let result = ewa::equally_weighted_average(&df);
    assert!(result.is_err());
}

#[test]
fn test_main() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
    ];
    let df = DataFrame { dates, data };

    let result = ewa::main(&df).unwrap();
    assert_eq!(result.dates.len(), 2);
    assert_eq!(result.data.len(), 2);
    assert_relative_eq!(result.data[0][0], 2.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][0], 5.0, epsilon = 1e-6);
}
