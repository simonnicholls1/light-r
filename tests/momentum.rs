use light_r::{DataFrame, operations::momentum};
use chrono::NaiveDate;
use approx::assert_relative_eq;

#[test]
fn test_calculate_momentum() {
    let dates: Vec<NaiveDate> = (0..10).map(|i| NaiveDate::from_ymd_opt(2021, 1, i+1).unwrap()).collect();
    let data = vec![
        vec![100.0, 200.0], vec![102.0, 202.0], vec![104.0, 204.0], vec![106.0, 206.0], vec![108.0, 208.0],
        vec![110.0, 210.0], vec![112.0, 212.0], vec![114.0, 214.0], vec![116.0, 216.0], vec![118.0, 218.0]
    ];
    let column_names = vec!["A".to_string(), "B".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = momentum::calculate_momentum(&df, 5, 2).unwrap();

    assert_eq!(result.dates.len(), 3);
    assert_eq!(result.data.len(), 3);
    
    // Check momentum values (these are approximate due to floating-point calculations)
    assert_relative_eq!(result.data[0][0], 0.10, epsilon = 1e-6); // (110 - 100) / 100
    assert_relative_eq!(result.data[0][1], 0.05, epsilon = 1e-6); // (210 - 200) / 200
    assert_relative_eq!(result.data[1][0], 0.10, epsilon = 1e-6); // (114 - 104) / 104
    assert_relative_eq!(result.data[1][1], 0.05, epsilon = 1e-6); // (214 - 204) / 204
    assert_relative_eq!(result.data[2][0], 0.0925925925925926, epsilon = 1e-6); // (118 - 108) / 108
    assert_relative_eq!(result.data[2][1], 0.0481927710843373, epsilon = 1e-6); // (218 - 208) / 208
}

#[test]
fn test_momentum_empty_df() {
    let df = DataFrame {
        dates: vec![],
        data: vec![],
        column_names: vec![],
    };

    let result = momentum::main(&df, 5, 2);
    assert!(result.is_err());
}

#[test]
fn test_momentum_frequency_one() {
    let dates: Vec<NaiveDate> = (0..6).map(|i| NaiveDate::from_ymd_opt(2021, 1, i+1).unwrap()).collect();
    let data = vec![
        vec![100.0], vec![102.0], vec![104.0], vec![106.0], vec![108.0], vec![110.0]
    ];
    let column_names = vec!["A".to_string()];
    let df = DataFrame { dates, data, column_names };

    let result = momentum::calculate_momentum(&df, 2, 1).unwrap();

    assert_eq!(result.dates.len(), 4);
    assert_eq!(result.data.len(), 4);
    
    assert_relative_eq!(result.data[0][0], 0.04, epsilon = 1e-6); // (104 - 100) / 100
    assert_relative_eq!(result.data[1][0], 0.0392156862745098, epsilon = 1e-6); // (106 - 102) / 102
    assert_relative_eq!(result.data[2][0], 0.0384615384615385, epsilon = 1e-6); // (108 - 104) / 104
    assert_relative_eq!(result.data[3][0], 0.0377358490566038, epsilon = 1e-6); // (110 - 106) / 106
}
