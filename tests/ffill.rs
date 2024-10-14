use light_r::{DataFrame, operations::ffill};
use chrono::NaiveDate;
use approx::assert_relative_eq;

#[test]
fn test_ffill() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 4).unwrap(),
    ];
    let data = vec![
        vec![1.0, f64::NAN],
        vec![f64::NAN, 2.0],
        vec![3.0, f64::NAN],
        vec![f64::NAN, 4.0],
    ];
    let df = DataFrame { dates, data };

    let result = ffill::ffill(&df);
    
    assert_eq!(result.dates.len(), 4);
    assert_eq!(result.data.len(), 4);
    assert_relative_eq!(result.data[0][0], 1.0, epsilon = 1e-6);
    assert!(result.data[0][1].is_nan());
    assert_relative_eq!(result.data[1][0], 1.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][1], 2.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[2][0], 3.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[2][1], 2.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[3][0], 3.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[3][1], 4.0, epsilon = 1e-6);
}

#[test]
fn test_ffill_all_nan() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data = vec![
        vec![f64::NAN, f64::NAN],
        vec![f64::NAN, f64::NAN],
    ];
    let df = DataFrame { dates, data };

    let result = ffill::ffill(&df);
    
    assert_eq!(result.dates.len(), 2);
    assert_eq!(result.data.len(), 2);
    assert!(result.data[0][0].is_nan());
    assert!(result.data[0][1].is_nan());
    assert!(result.data[1][0].is_nan());
    assert!(result.data[1][1].is_nan());
}

#[test]
fn test_main() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data = vec![
        vec![1.0, f64::NAN],
        vec![f64::NAN, 2.0],
    ];
    let df = DataFrame { dates, data };

    let result = ffill::main(&df);
    assert_eq!(result.dates.len(), 2);
    assert_eq!(result.data.len(), 2);
    assert_relative_eq!(result.data[0][0], 1.0, epsilon = 1e-6);
    assert!(result.data[0][1].is_nan());
    assert_relative_eq!(result.data[1][0], 1.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][1], 2.0, epsilon = 1e-6);
}
