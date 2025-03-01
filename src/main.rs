use std::{
    fs::File,
    path::Path,
    env,
};

use chrono::{Duration, Local, NaiveDate, Timelike, Utc};
use dotenv::dotenv;
use influxdb::{Client, ReadQuery};
use polars::prelude::*;
use serde_json::Value;
use tokio::time::sleep;

async fn query_influx_data(
    url: &str,db: &str,measur: &str,device: &str,start: &str,end: &str) 
    -> Result<String, Box<dyn std::error::Error>> {
    let sentence = format!(r#"
        SELECT *
        FROM "{}"
        WHERE device = '{}'
        AND time >= '{}' AND time <= '{}'"#,
        measur, device, start, end
    );

    println!("{}", sentence);

    let client = Client::new(url, db);
    let read_query = ReadQuery::new(sentence);
    let result = client.query(read_query).await?;

    Ok(result)
}

async fn compile_surface_data(
    values: &Vec<Value>, current_date: NaiveDate, device: &str) -> Result<(), Box<dyn std::error::Error>> {
    let time: Vec<String> = values
    .iter().map(|influx_data| influx_data[0].as_str().unwrap().to_string()).collect();
    let bat_volt: Vec<f64> = values
    .iter().map(|influx_data| influx_data[1].as_f64().unwrap()).collect();
    let temp_bot: Vec<f64> = values
    .iter().map(|influx_data| influx_data[5].as_f64().unwrap()).collect();
    let temp_top: Vec<f64> = values
    .iter().map(|influx_data| influx_data[6].as_f64().unwrap()).collect();

    let mut df_influx = DataFrame::new(vec![
        Column::new("timestamp".into(), time),
        Column::new("bat_volt".into(), bat_volt),
        Column::new("temp_bot_c".into(), temp_bot),
        Column::new("temp_top_c".into(), temp_top),
    ]).unwrap();

    let filename = &mut current_date.to_string().to_owned();
    filename.push_str("-");
    filename.push_str(device);
    filename.push_str(".csv");

    let file_path = Path::new(&filename);
    let file = File::create(&file_path)?;
    CsvWriter::new(file).finish(&mut df_influx)?;

    Ok(())
}

async fn compile_ambient_data(
    values: &Vec<Value>, current_date: NaiveDate, device: &str) -> Result<(), Box<dyn std::error::Error>> {
    
    let time: Vec<String> = values
    .iter().map(|influx_data| influx_data[0].as_str().unwrap().to_string()).collect();
    let bat_volt: Vec<f64> = values
    .iter().map(|influx_data| influx_data[1].as_f64().unwrap()).collect();
    let humidity: Vec<f64> = values
    .iter().map(|influx_data| influx_data[4].as_f64().unwrap()).collect();
    let light: Vec<f64> = values
    .iter().map(|influx_data| influx_data[6].as_f64().unwrap()).collect();
    let rain: Vec<f64> = values
    .iter().map(|influx_data| influx_data[7].as_f64().unwrap()).collect();
    let temp: Vec<f64> = values
    .iter().map(|influx_data| influx_data[8].as_f64().unwrap()).collect();

    let mut df_influx = DataFrame::new(vec![
        Column::new("timestamp".into(), time),
        Column::new("bat_volt".into(), bat_volt),
        Column::new("hum_%".into(), humidity),
        Column::new("light_lux".into(), light),
        Column::new("rain_rate".into(), rain),
        Column::new("temp_c".into(), temp),
    ]).unwrap();

    let filename = &mut current_date.to_string().to_owned();
    filename.push_str("-");
    filename.push_str(device);
    filename.push_str(".csv");

    let file_path = Path::new(&filename);
    let file = File::create(&file_path)?;
    CsvWriter::new(file).finish(&mut df_influx)?;

    Ok(())
}

async fn query_influx_acdc(
    url: &str,db: &str,measur: &str,channel: &str,start: &str,end: &str)
    -> Result<String, Box<dyn std::error::Error>> {
    let sentence = format!(r#"
        SELECT *
        FROM "{}"
        WHERE channel = '{}'
        AND time >= '{}' AND time <= '{}'"#,
        measur, channel, start, end
    );
    println!("{}", sentence);

    let client = Client::new(url, db);
    let read_query = ReadQuery::new(sentence);
    let result = client.query(read_query).await?;

    Ok(result)
}

async fn compile_ac_data(
    values: &Vec<Value>, current_date: NaiveDate, channel: &str) -> Result<(), Box<dyn std::error::Error>> {
    
    let time: Vec<String> = values
    .iter().map(|influx_data| influx_data[0].as_str().unwrap().to_string()).collect();
    let current: Vec<f64> = values
    .iter().map(|influx_data| influx_data[2].as_f64().unwrap()).collect();
    let energy: Vec<f64> = values
    .iter().map(|influx_data| influx_data[4].as_f64().unwrap()).collect();
    let freq: Vec<f64> = values
    .iter().map(|influx_data| influx_data[5].as_f64().unwrap()).collect();
    let pf: Vec<f64> = values
    .iter().map(|influx_data| influx_data[7].as_f64().unwrap()).collect();
    let power: Vec<f64> = values
    .iter().map(|influx_data| influx_data[8].as_f64().unwrap()).collect();
    let voltage: Vec<f64> = values
    .iter().map(|influx_data| influx_data[9].as_f64().unwrap()).collect();
    

    let mut df_influx = DataFrame::new(vec![
        Column::new("timestamp".into(), time),
        Column::new("current".into(), current),
        Column::new("energy".into(), energy),
        Column::new("freq".into(), freq),
        Column::new("pf".into(), pf),
        Column::new("power".into(), power),
        Column::new("voltage".into(), voltage),
    ]).unwrap();

    let filename = &mut current_date.to_string().to_owned();
    filename.push_str("-");
    filename.push_str(channel);
    filename.push_str(".csv");

    let file_path = Path::new(&filename);
    let file = File::create(&file_path)?;
    CsvWriter::new(file).finish(&mut df_influx)?;

    Ok(())
}

async fn compile_dc_data(
    values: &Vec<Value>, current_date: NaiveDate, channel: &str) -> Result<(), Box<dyn std::error::Error>> {
    
    let time: Vec<String> = values
    .iter().map(|influx_data| influx_data[0].as_str().unwrap().to_string()).collect();
    let current: Vec<f64> = values
    .iter().map(|influx_data| influx_data[2].as_f64().unwrap()).collect();
    let voltage: Vec<f64> = values
    .iter().map(|influx_data| influx_data[9].as_f64().unwrap()).collect();
    

    let mut df_influx = DataFrame::new(vec![
        Column::new("timestamp".into(), time),
        Column::new("current".into(), current),
        Column::new("voltage".into(), voltage),
    ]).unwrap();

    let filename = &mut current_date.to_string().to_owned();
    filename.push_str("-");
    filename.push_str(channel);
    filename.push_str(".csv");

    let file_path = Path::new(&filename);
    let file = File::create(&file_path)?;
    CsvWriter::new(file).finish(&mut df_influx)?;

    Ok(())
}

fn get_next_run_time() -> chrono::DateTime<Utc> {
    let now = Utc::now();

    // Get the next minute, but set the second to 30
    let next_minute = now
        .with_second(0)     // Reset seconds to 0
        .unwrap()
        .with_nanosecond(0) // Reset nanoseconds to 0
        .unwrap()
        + chrono::Duration::minutes(1); // Move to the next minute

    // Set the second to 30 of the next minute
    next_minute.with_second(30).unwrap()
}

async fn routine() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let inf_url = env::var("INFLUX_URL").expect("InfluxDB url must be set");
    let inf_db = env::var("INFLUX_DB").expect("InfluxDB database must be set");
    let inf_measure = env::var("INFLUX_MEASURE").expect("InfluxDB measurement must be set");
    let inf_device_sur = env::var("INFLUX_DEVICE_SUR").expect("InfluxDB device:surface must be set");
    let inf_device_amb = env::var("INFLUX_DEVICE_AMB").expect("InfluxDB device:ambient must be set");
    let inf_ch_ac = env::var("INFLUX_CH_AC").expect("InfluxDB channel:ac must be set");
    let inf_ch_dc = env::var("INFLUX_CH_DC").expect("InfluxDB channel:dc must be set");
    
    let measure_array: Vec<&str> = inf_measure.split(',').collect();
    let device_sur_array: Vec<&str> = inf_device_sur.split(',').collect();
    let device_amb_array: Vec<&str> = inf_device_amb.split(',').collect();
    let ch_ac_array: Vec<&str> = inf_ch_ac.split(',').collect();
    let ch_dc_array: Vec<&str> = inf_ch_dc.split(',').collect();

    let current_date = Local::now().date_naive();
    let yesterday_date = current_date - Duration::days(30);
    let start_day = match yesterday_date.and_hms_opt(0, 0, 0) {
        Some(v) => v.to_string(),
        None => String::from("None"),
    };
    let end_day = match current_date.and_hms_opt(23, 59, 59) {
        Some(v) => v.to_string(),
        None => String::from("None"),
    };

    for device_sur in &device_sur_array {
        let influx_data_str = query_influx_data(&inf_url, &inf_db, &measure_array[0], &device_sur, &start_day, &end_day).await?;
        let influx_data_json: Value = serde_json::from_str(&influx_data_str)?;
        let series = &influx_data_json["results"][0]["series"][0];
        let values = series["values"].as_array().unwrap();
        compile_surface_data(values, current_date, device_sur).await?;
    }
    
    for device_amb in &device_amb_array {
        let influx_data_str = query_influx_data(&inf_url, &inf_db, &measure_array[1], &device_amb, &start_day, &end_day).await?;
        let influx_data_json: Value = serde_json::from_str(&influx_data_str)?;
        let series = &influx_data_json["results"][0]["series"][0];
        let values = series["values"].as_array().unwrap();
        compile_ambient_data(values, current_date, device_amb).await?;
    }
    
    for ch_ac in &ch_ac_array {
        let influx_data_str = query_influx_acdc(&inf_url, &inf_db, &measure_array[2], &ch_ac, &start_day, &end_day).await?;
        let influx_data_json: Value = serde_json::from_str(&influx_data_str)?;
        let series = &influx_data_json["results"][0]["series"][0];
        let values = series["values"].as_array().unwrap();
        compile_ac_data(values, current_date, &ch_ac).await?;
    }
    
    for ch_dc in &ch_dc_array {
        let influx_data_str = query_influx_acdc(&inf_url, &inf_db, &measure_array[2], &ch_dc, &start_day, &end_day).await?;
        let influx_data_json: Value = serde_json::from_str(&influx_data_str)?;
        let series = &influx_data_json["results"][0]["series"][0];
        let values = series["values"].as_array().unwrap();
        // let columns = series["columns"].as_array().unwrap();
        // println!("{:?}", columns); 
        compile_dc_data(values, current_date, &ch_dc).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    loop {
        let next_run = get_next_run_time();
        let delay = next_run.signed_duration_since(Utc::now()).to_std().unwrap();

        // Sleep until the 30th second of the next minute
        sleep(delay).await;

        // Execute your task on the 30th second of every minute
        let _ = routine().await;
    }
}