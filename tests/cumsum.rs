use chrono::NaiveDate;
use light_r::{DataFrame, operations::cumsum};

#[test]
fn test_cumulative_sum() {
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
    let mut df = DataFrame { dates, data };

    let result = cumsum::cumulative_sum(&mut df, 10.0);
    assert_eq!(result.data[0], vec![11.0, 12.0, 13.0]);
    assert_eq!(result.data[1], vec![15.0, 17.0, 19.0]);
    assert_eq!(result.data[2], vec![22.0, 25.0, 28.0]);
}

#[test]
fn test_cumulative_sum_with_nan() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 3).unwrap(),
    ];
    let data = vec![
        vec![1.0, f64::NAN, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];
    let mut df = DataFrame { dates, data };

    let result = cumsum::cumulative_sum(&mut df, 10.0);
    assert_eq!(result.data.len(), 2);  // NaN row should be removed
    assert_eq!(result.data[0], vec![14.0, 15.0, 16.0]);
    assert_eq!(result.data[1], vec![21.0, 23.0, 25.0]);
}

#[test]
fn test_main() {
    let dates = vec![
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2021, 1, 2).unwrap(),
    ];
    let data = vec![
        vec![1.0, 2.0],
        vec![3.0, 4.0],
    ];
    let mut df = DataFrame { dates, data };

    let result = cumsum::main(&mut df, 5.0).unwrap();
    assert_eq!(result.data[0], vec![6.0, 7.0]);
    assert_eq!(result.data[1], vec![9.0, 11.0]);
}
