use chrono::NaiveDate;
use light_r::{DataFrame, operations::before};

#[test]
fn test_before_date() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 4).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 5).unwrap(),
    ];
    let data = vec![vec![1.0, 2.0, 3.0, 4.0, 5.0]];
    let df = DataFrame { dates, data };

    let result = before::before_date(&df, NaiveDate::from_ymd_opt(2021, 1, 4).unwrap());
    assert_eq!(result.dates.len(), 3);
    assert_eq!(result.dates.last().unwrap(), &NaiveDate::from_ymd_opt(2021, 1, 3).unwrap());
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

    let result = before::main(&df, "2021-01-04").unwrap();
    assert_eq!(result.dates.len(), 3);
    assert_eq!(result.dates.last().unwrap(), &NaiveDate::from_ymd_opt(2021, 1, 3).unwrap());
}
