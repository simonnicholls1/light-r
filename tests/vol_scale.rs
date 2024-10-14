use light_r::{DataFrame, operations::vol_scale};
use chrono::NaiveDate;
use approx::assert_relative_eq;

#[test]
fn test_vol_scale() {
    let dates: Vec<NaiveDate> = (0..10).map(|i| NaiveDate::from_ymd_opt(2021, 1, i+1).unwrap()).collect();
    let data = vec![
        vec![1.0, 2.0], vec![2.0, 3.0], vec![3.0, 4.0], vec![4.0, 5.0], vec![5.0, 6.0],
        vec![6.0, 7.0], vec![7.0, 8.0], vec![8.0, 9.0], vec![9.0, 10.0], vec![10.0, 11.0]
    ];
    let column_names = vec!["A".to_string(), "B".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = vol_scale::main(&df, 5, 0.1).unwrap();

    // The first 4 rows should be NaN due to the window size
    for i in 0..4 {
        assert!(result.data[i][0].is_nan());
        assert!(result.data[i][1].is_nan());
    }

    // Check a few values (these are approximate due to floating-point calculations)
    assert_relative_eq!(result.data[5][0], 0.3464, epsilon = 1e-4);
    assert_relative_eq!(result.data[5][1], 0.4041, epsilon = 1e-4);
    assert_relative_eq!(result.data[9][0], 0.2739, epsilon = 1e-4);
    assert_relative_eq!(result.data[9][1], 0.3015, epsilon = 1e-4);
}

#[test]
fn test_vol_scale_empty_df() {
    let df = DataFrame {
        dates: vec![],
        data: vec![],
        column_names: vec![],
    };

    let result = vol_scale::main(&df, 5, 0.1);
    assert!(result.is_err());
}

#[test]
fn test_vol_scale_window_too_large() {
    let dates = vec![NaiveDate::from_ymd_opt(2021, 1, 1).unwrap()];
    let data = vec![vec![1.0]];
    let column_names = vec!["A".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = vol_scale::main(&df, 5, 0.1).unwrap();
    assert!(result.data[0][0].is_nan());
}
