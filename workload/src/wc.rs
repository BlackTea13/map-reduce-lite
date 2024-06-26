//! A MapReduce-compatible implementation of word count.
//!

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use common::{KeyValue, MapOutput};

pub fn map(kv: KeyValue, _aux: Bytes) -> MapOutput {
    let s = String::from_utf8(kv.value.as_ref().into())?;
    let words = s
        .split(|c: char| !c.is_alphabetic())
        .filter(|s| !s.is_empty())
        .map(|word| word.to_lowercase())
        .collect::<Vec<_>>();

    let mut key_buf = BytesMut::new();
    let mut value_buf = BytesMut::with_capacity(words.len() * 8);

    let iter = words.into_iter().map(move |word| {
        key_buf.put_slice(word.into_bytes().as_ref());
        value_buf.put_u64(1);

        let key = key_buf.split().freeze();
        let value = value_buf.split().freeze();

        Ok(KeyValue { key, value })
    });
    Ok(Box::new(iter))
}

pub fn reduce(
    key: Bytes,
    values: Box<dyn Iterator<Item=Bytes> + '_>,
    _aux: Bytes,
) -> Result<Bytes> {
    let count: u64 = values.map(|mut value| value.get_u64()).sum();

    let mut writer = BytesMut::with_capacity(8);
    let key = String::from_utf8(key.to_vec())?;
    writer.put(format!("{} {}\n", key, count).as_bytes());

    Ok(writer.freeze())
}
