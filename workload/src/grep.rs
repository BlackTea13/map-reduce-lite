//! A MapReduce-compatible implementation of `grep`.
//!

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Parser;
use common::{KeyValue, MapOutput};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug, Serialize, Deserialize)]
#[clap(no_binary_name = true)]
struct Args {
    #[clap(short, long, value_parser)]
    term: String,
}

#[allow(clippy::needless_collect)]
pub fn map(kv: KeyValue, aux: Bytes) -> MapOutput {
    let args = Args::try_parse_from(serde_json::from_slice::<Vec<String>>(&aux)?)?;
    let term = args.term;

    let s = String::from_utf8(kv.value.as_ref().into())?;
    let lines = s
        .lines()
        .enumerate()
        .filter(|(_, s)| s.contains(&term))
        .map(|(i, s)| (i + 1, s.to_string()))
        .collect::<Vec<_>>();

    let mut key_buf = BytesMut::new();
    let mut value_buf = BytesMut::new();

    let iter = lines.into_iter().map(move |(line_num, line)| {
        key_buf.put(kv.key.as_ref());
        value_buf.put_u64(line_num as u64);
        value_buf.put(line.as_bytes());

        let key = key_buf.split().freeze();
        let value = value_buf.split().freeze();

        Ok(KeyValue { key, value })
    });
    Ok(Box::new(iter))
}

pub fn reduce(
    key: Bytes,
    values: Box<dyn Iterator<Item = Bytes> + '_>,
    _aux: Bytes,
) -> Result<Bytes> {
    let mut writer = BytesMut::with_capacity(8);
    let mut values = values.collect::<Vec<Bytes>>();
    values.sort_by_key(|value| value.clone().get_u64());
    for mut value in values {
        let filename = String::from_utf8(key.to_vec())?;
        let line_no = value.get_u64();
        let line = String::from_utf8(value.to_vec())?;
        writer.put(format!("{}:{}:: {}\n", filename, line_no, line).as_bytes());
    }

    Ok(writer.freeze())
}
