use chrono::NaiveDate;
use light_r::{DataFrame, operations::after};

#[test]
fn test_after_date() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 4).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 5).unwrap(),
    ];
    let data = vec![vec![1.0, 2.0, 3.0, 4.0, 5.0]];
    let column_names = vec!["date", "return"];
    let df = DataFrame {dates, data, column_names};

    let result = after::after_date(&df, NaiveDate::from_ymd_opt(2021, 1, 3).unwrap());
    assert_eq!(result.dates.len(), 2);
    assert_eq!(result.dates[0], NaiveDate::from_ymd_opt(2021, 1, 4).unwrap());
}

#[test]
fn test_main() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 4).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 5).unwrap(),
    ];
    let data = vec![vec![1.0, 2.0, 3.0, 4.0, 5.0]];
    let df = DataFrame { dates, data };

    let result = after::main(&df, "2021-01-03").unwrap();
    assert_eq!(result.dates.len(), 2);
    assert_eq!(result.dates[0], NaiveDate::from_ymd_opt(2021, 1, 4).unwrap());
}
