use std::fmt::Display;
use std::io::BufRead;

/// MapReduce application for matrix multiplication.
/// Composed of two stages.
use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use clap::builder::TypedValueParser;

use common::{KeyValue, MapOutput};

pub fn map_phase_one(kv: KeyValue, _aux: Bytes) -> MapOutput {
    let contents = String::from_utf8(kv.value.as_ref().into())?;
    let lines = contents
        .lines()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    let mut key_buf = BytesMut::with_capacity(lines.len() * 4);
    let mut value_buf = BytesMut::new();

    let iter = lines.into_iter().map(move |line| {
        let split = line.split(' ').collect::<Vec<_>>();

        if split.len() != 4 {
            return Err(anyhow!("input was not in the expected format"));
        }

        let row = split[0];
        let column = split[1];
        let value = split[2];
        let matrix = split[3];

        if matrix.eq("A") {
            key_buf.put_slice(column.as_bytes());
            value_buf.put_slice(row.as_bytes());
            value_buf.put_slice(" ".as_bytes());
        } else if matrix.eq("B") {
            key_buf.put_slice(row.as_bytes());
            value_buf.put_slice(column.as_bytes());
            value_buf.put_slice(" ".as_bytes());
        } else {
            return Err(anyhow!("unexpected matrix name"));
        }

        value_buf.put_slice(value.as_bytes());
        value_buf.put_slice(" ".as_bytes());
        value_buf.put_slice(matrix.as_bytes());

        let key = key_buf.split().freeze();
        let value = value_buf.split().freeze();

        Ok(KeyValue { key, value })
    });
    Ok(Box::new(iter))
}

pub fn reduce_phase_one(
    key: Bytes,
    values: Box<dyn Iterator<Item = Bytes> + '_>,
    _aux: Bytes,
) -> Result<Bytes> {
    let rows = values.collect::<Vec<Bytes>>();

    let mut strings = Vec::new();
    for row in rows {
        let string = String::from_utf8(row.to_vec())?;
        strings.push(string);
    }

    let matrix_a = strings
        .iter()
        .map(|s| s.split(" ").collect::<Vec<_>>())
        .filter(|s| s.last().unwrap() == &"A")
        .collect::<Vec<_>>();
    let matrix_b = strings
        .iter()
        .map(|s| s.split(" ").collect::<Vec<_>>())
        .filter(|s| s.last().unwrap() == &"B")
        .collect::<Vec<_>>();

    let mut buffer = BytesMut::new();


    for row_a in &matrix_a {
        let row = *row_a.first().unwrap();

        for row_b in &matrix_b {
            let val_a = str::parse::<f64>(row_a[1])?;
            let val_b = str::parse::<f64>(row_b[1])?;
            let prod = val_a * val_b;

            let row = format!("{} {} {} {}", row, row_b[0], prod, "C");
            buffer.put_slice(row.as_bytes());
            buffer.put_slice(b"\n");
        }
    }

    let bytes = buffer.freeze();
    Ok(bytes)
}

pub fn map_phase_two(kv: KeyValue, _aux: Bytes) -> MapOutput {
    let contents = String::from_utf8(kv.value.as_ref().into())?;
    let lines = contents.lines().map(|s| s.to_string()).collect::<Vec<_>>();

    let mut key_buf = BytesMut::new();
    let mut value_buf = BytesMut::new();

    let iter = lines.into_iter().map(move |line| {
        let split = line.split(" ").collect::<Vec<_>>();

        key_buf.put_slice(split[0].as_bytes());
        key_buf.put_slice(" ".as_bytes());
        key_buf.put_slice(split[1].as_bytes());

        value_buf.put_slice(split[2].as_bytes());
        value_buf.put_slice(" ".as_bytes());
        value_buf.put_slice(split[3].as_bytes());

        let key = key_buf.split().freeze();
        let value = value_buf.split().freeze();

        Ok(KeyValue { key, value })
    });

    Ok(Box::new(iter))
}

pub fn reduce_phase_two(
    key: Bytes,
    values: Box<dyn Iterator<Item = Bytes> + '_>,
    _aux: Bytes,
) -> Result<Bytes> {
    let rows = values.collect::<Vec<_>>();

    let mut sum: f64 = 0.0;
    for row in rows {
        let string = String::from_utf8(row.to_vec())?;
        let split = string.split(" ").collect::<Vec<_>>();
        let value = str::parse::<f64>(split[0])?;
        sum = sum + value;
    }

    let mut buffer = BytesMut::new();

    buffer.put_slice(key.as_ref());
    buffer.put_slice(b" ");
    buffer.put_slice(sum.to_string().as_bytes());
    buffer.put_slice(b" ");
    buffer.put_slice("C".as_bytes());
    buffer.put_slice(b"\n");

    let bytes = buffer.freeze();

    Ok(bytes)
}
