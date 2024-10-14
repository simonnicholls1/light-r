use chrono::NaiveDate;
use light_r::{DataFrame, operations::cgrep};

#[test]
fn test_filter_columns() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
    ];
    let column_names = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = cgrep::filter_columns(&df, &["A".to_string(), "C".to_string()]);
    assert_eq!(result.column_names, vec!["A".to_string(), "C".to_string()]);
    assert_eq!(result.data, vec![vec![1.0, 3.0], vec![4.0, 6.0]]);
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
    let column_names = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = cgrep::main(&df, &["A".to_string(), "C".to_string()]).unwrap();
    assert_eq!(result.column_names, vec!["A".to_string(), "C".to_string()]);
    assert_eq!(result.data, vec![vec![1.0, 3.0], vec![4.0, 6.0]]);
}

#[test]
fn test_main_empty_data() {
    let df = DataFrame {
        dates: vec![],
        data: vec![],
        column_names: vec![],
    };
    let result = cgrep::main(&df, &["A".to_string()]);
    assert!(result.is_err());
}
