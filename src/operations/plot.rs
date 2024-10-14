use crate::DataFrame;
use std::error::Error;
use std::fs::File;
use std::io::Write;

pub fn plot(df: &DataFrame) -> Result<(), Box<dyn Error>> {
    let mut html_content = String::from(r#"
<!DOCTYPE html>
<html>
<head>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
    <div id="plot"></div>
    <script>
        var data = [
"#);

    for (i, column_name) in df.column_names.iter().enumerate() {
        let x_values: Vec<String> = df.dates.iter().map(|d| d.format("%Y-%m-%d").to_string()).collect();
        let y_values: Vec<f64> = df.data.iter().map(|row| row[i]).collect();

        html_content.push_str(&format!(r#"
            {{
                x: {:?},
                y: {:?},
                type: 'scatter',
                mode: 'lines',
                name: '{}'
            }},"#, x_values, y_values, column_name));
    }

    html_content.push_str(r#"
        ];

        var layout = {
            title: 'Signal p&l',
            xaxis: { title: 'Date' },
            yaxis: { title: 'Value' }
        };

        Plotly.newPlot('plot', data, layout);
    </script>
</body>
</html>
"#);

    let mut file = File::create("plot.html")?;
    file.write_all(html_content.as_bytes())?;

    println!("Plot has been generated in 'plot.html'. Please open this file in a web browser to view the plot.");

    let ann_return = annualized_return(df);
    let ann_vol = annualized_volatility(df);
    let sharpe = sharpe_ratio(df, ann_return, ann_vol);
    let max_dd = max_drawdown(df);

    println!("Annualized Return: {:.2}%", ann_return * 100.0);
    println!("Annualized Volatility: {:.2}%", ann_vol * 100.0);
    println!("Sharpe Ratio: {:.2}", sharpe);
    println!("Maximum Drawdown: {:.2}%", max_dd * 100.0);

    Ok(())
}

pub fn main(df: &DataFrame) -> Result<(), Box<dyn Error>> {
    plot(df)
}


fn annualized_return(df: &DataFrame) -> f64 {
    let num_years = df.dates.len() as f64 / 252.0; // Assuming 252 trading days per year
    let cumulative_return = df.data.last().map(|row| row[0]).unwrap_or(1.0);
    cumulative_return.powf(1.0 / num_years) - 1.0
}

fn annualized_volatility(df: &DataFrame) -> f64 {
    let num_years = df.dates.len() as f64 / 252.0; // Assuming 252 trading days per year
    let returns: Vec<f64> = df.data.windows(2).map(|window| window[1][0] / window[0][0] - 1.0).collect();
    let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns.iter().map(|&r| (r - mean_return).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;
    (variance * 252.0).sqrt() / num_years.sqrt()
}

fn sharpe_ratio(df: &DataFrame, ann_return: f64, ann_vol: f64) -> f64 {
    if ann_vol == 0.0 {
        0.0
    } else {
        ann_return / ann_vol
    }
}

fn max_drawdown(df: &DataFrame) -> f64 {
    let mut max_equity = 0.0;
    let mut max_drawdown = 0.0;

    for row in &df.data {
        let equity = row[0];
        if equity > max_equity {
            max_equity = equity;
        }
        let drawdown = (equity - max_equity) / max_equity;
        if drawdown < max_drawdown {
            max_drawdown = drawdown;
        }
    }

    max_drawdown
}
