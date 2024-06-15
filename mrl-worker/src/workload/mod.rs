use bytes::Bytes;
use tracing::debug;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct KeyValue {
    /// The key.
    pub key: Bytes,

    /// The value.
    pub value: Bytes,
}

impl KeyValue {
    /// Construct a new key-value pair from the given key and value.
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    /// Get the key of this key-value pair.
    ///
    /// This method is cheap, since [`Bytes`] are cheaply cloneable.
    #[inline]
    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    /// Get the value of this key-value pair.
    ///
    /// This method is cheap, since [`Bytes`] are cheaply cloneable.
    #[inline]
    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    /// Consumes the key-value pair and returns the key.
    #[inline]
    pub fn into_key(self) -> Bytes {
        self.key
    }

    /// Consumes the key-value pair and returns the value.
    #[inline]
    pub fn into_value(self) -> Bytes {
        self.value
    }
}

pub fn map(kv: KeyValue, aux: Bytes) {
    debug!("map ran!")
}

pub fn reduce(key: Bytes, aux: Bytes) {
    debug!("reduce ran!")
}
