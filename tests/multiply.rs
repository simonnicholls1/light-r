use light_r::{DataFrame, operations::multiply};
use chrono::NaiveDate;
use approx::assert_relative_eq;

#[test]
fn test_multiply() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data1 = vec![
        vec![1.0, 2.0],
        vec![3.0, 4.0],
    ];
    let data2 = vec![
        vec![2.0, 3.0],
        vec![4.0, 5.0],
    ];
    let df1 = DataFrame { dates: dates.clone(), data: data1, column_names: vec!["A".to_string(), "B".to_string()] };
    let df2 = DataFrame { dates, data: data2, column_names: vec!["A".to_string(), "B".to_string()] };

    let result = multiply::multiply(&df1, &df2).unwrap();
    
    assert_eq!(result.dates.len(), 2);
    assert_eq!(result.data.len(), 2);
    assert_relative_eq!(result.data[0][0], 2.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[0][1], 6.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][0], 12.0, epsilon = 1e-6);
    assert_relative_eq!(result.data[1][1], 20.0, epsilon = 1e-6);
}

#[test]
fn test_multiply_mismatched_dates() {
    let dates1 = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let dates2 = vec![NaiveDate::from_ymd_opt(2021, 1, 2).unwrap()];
    let data = vec![vec![1.0]];
    let df1 = DataFrame { dates: dates1, data: data.clone(), column_names: vec!["A".to_string()] };
    let df2 = DataFrame { dates: dates2, data, column_names: vec!["A".to_string()] };

    let result = multiply::multiply(&df1, &df2);
    assert!(result.is_err());
}

#[test]
fn test_multiply_mismatched_columns() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data1 = vec![vec![1.0]];
    let data2 = vec![vec![1.0, 2.0]];
    let df1 = DataFrame { dates: dates.clone(), data: data1, column_names: vec!["A".to_string()] };
    let df2 = DataFrame { dates, data: data2, column_names: vec!["A".to_string(), "B".to_string()] };

    let result = multiply::multiply(&df1, &df2);
    assert!(result.is_err());
}

#[test]
fn test_main() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data1 = vec![vec![2.0]];
    let data2 = vec![vec![3.0]];
    let df1 = DataFrame { dates: dates.clone(), data: data1, column_names: vec!["A".to_string()] };
    let df2 = DataFrame { dates, data: data2, column_names: vec!["A".to_string()] };

    let result = multiply::main(&df1, &df2).unwrap();
    assert_eq!(result.dates.len(), 1);
    assert_eq!(result.data.len(), 1);
    assert_relative_eq!(result.data[0][0], 6.0, epsilon = 1e-6);
}
