use crate::{error, value};
use loro::{Container, LoroMap, ValueOrContainer};
use magnus::{value::ReprValue, Error, RArray, RHash, Ruby, Value};

/// A Loro container handle owns an internal reference to its document. No Ruby
/// object needs to be marked to keep the document alive.
#[magnus::wrap(class = "Loro::Map", free_immediately, size)]
pub struct Map {
    inner: LoroMap,
}

impl Map {
    pub fn from_inner(inner: LoroMap) -> Self {
        Self { inner }
    }

    pub fn set(&self, key: Value, item: Value) -> Result<(), Error> {
        let key = value::key_to_string(key)?;
        let item = value::ruby_to_loro(item)?;
        self.inner.insert(&key, item).map_err(error::loro)
    }

    pub fn get(&self, key: Value) -> Result<Value, Error> {
        let key = value::key_to_string(key)?;
        match self.inner.get(&key) {
            Some(item) => value::value_or_container_to_ruby(&item),
            None => Ok(Ruby::get().unwrap().qnil().as_value()),
        }
    }

    pub fn get_map(&self, key: Value) -> Result<Option<Self>, Error> {
        let key = value::key_to_string(key)?;
        Ok(match self.inner.get(&key) {
            Some(ValueOrContainer::Container(Container::Map(map))) => Some(Self::from_inner(map)),
            _ => None,
        })
    }

    /// Deterministic child map under `key`. Two peers that call this
    /// independently for the same (parent, key) get the SAME child container id,
    /// so their edits merge. `insert_container` is deliberately not exposed: it
    /// mints an op-id child, and concurrent first-writes at one key then fork —
    /// one peer's container silently overwrites the other's.
    pub fn ensure_mergeable_map(&self, key: Value) -> Result<Self, Error> {
        let key = value::key_to_string(key)?;
        let map = self
            .inner
            .ensure_mergeable_map(&key)
            .map_err(error::loro)?;
        Ok(Self::from_inner(map))
    }

    pub fn delete(&self, key: Value) -> Result<(), Error> {
        let key = value::key_to_string(key)?;
        self.inner.delete(&key).map_err(error::loro)
    }

    pub fn contains_key(&self, key: Value) -> Result<bool, Error> {
        let key = value::key_to_string(key)?;
        Ok(self.inner.get(&key).is_some())
    }

    pub fn keys(&self) -> Result<RArray, Error> {
        let ruby = Ruby::get().unwrap();
        let array = ruby.ary_new_capa(self.inner.len());
        for key in self.inner.keys() {
            array.push(ruby.str_new(&key))?;
        }
        Ok(array)
    }

    pub fn size(&self) -> usize {
        self.inner.len()
    }

    pub fn to_h(&self) -> Result<RHash, Error> {
        value::loro_map_to_ruby(&self.inner.get_deep_value())
    }
}
