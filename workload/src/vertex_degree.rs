//! A MapReduce-compatible application that computes the
//! degree of each vertex in a graph, given a list of edges.
//!

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};

use common::utils::string_from_bytes;
use common::{KeyValue, MapOutput};

fn parse_line(line: &str) -> Result<(u64, u64)> {
    let mut iter = line.split_whitespace().take(2);
    let a = iter
        .next()
        .ok_or_else(|| anyhow!("Invalid input file format"))?
        .parse()?;
    let b = iter
        .next()
        .ok_or_else(|| anyhow!("Invalid input file format"))?
        .parse()?;
    Ok((a, b))
}

pub fn map(kv: KeyValue, _aux: Bytes) -> MapOutput {
    let s = string_from_bytes(kv.value)?;
    let edges = s.lines().map(parse_line).collect::<Result<Vec<_>>>()?;

    // let mut key_buf = BytesMut::with_capacity(8 * edges.len());
    // let mut value_buf = BytesMut::with_capacity(8 * edges.len());

    let iter = edges.into_iter().flat_map(move |(a, b)| {
        [
            Ok(KeyValue {
                key: Bytes::from(a.to_string()),
                value: Bytes::from("1"),
            }),
            Ok(KeyValue {
                key: Bytes::from(b.to_string()),
                value: Bytes::from("1"),
            }),
        ]
    });
    Ok(Box::new(iter))
}

pub fn reduce(
    key: Bytes,
    values: Box<dyn Iterator<Item = Bytes> + '_>,
    _aux: Bytes,
) -> Result<Bytes> {
    let mut count = 0u64;

    for value in values {
        count += String::from_utf8(value.to_vec())?.parse::<u64>()?;
    }

    let mut value = BytesMut::with_capacity(24);
    let vertex_no = String::from_utf8(key.clone().to_vec())?;
    value.put(format!("{}, deg={}\n", &vertex_no, count).as_bytes());
    Ok(value.freeze())
}
