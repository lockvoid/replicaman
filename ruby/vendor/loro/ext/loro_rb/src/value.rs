use crate::error;
use loro::{LoroValue, ValueOrContainer};
use magnus::{
    encoding::EncodingCapable, prelude::*, r_hash::ForEach, Error, IntoValue, RArray, RHash,
    RString, Ruby, Symbol, TryConvert, Value,
};

pub fn key_to_string(value: Value) -> Result<String, Error> {
    if let Some(string) = RString::from_value(value) {
        return string.to_string();
    }

    if let Some(symbol) = Symbol::from_value(value) {
        return Ok(symbol.name()?.into_owned());
    }

    Err(error::value_type(format!(
        "map keys must be String or Symbol, got {}",
        unsafe { value.classname() }
    )))
}

pub fn ruby_to_loro(value: Value) -> Result<LoroValue, Error> {
    let ruby = Ruby::get_with(value);
    let seen = ruby.hash_new();
    let _: RHash = seen.funcall("compare_by_identity", ())?;
    ruby_to_loro_with_seen(value, seen)
}

fn ruby_to_loro_with_seen(value: Value, seen: RHash) -> Result<LoroValue, Error> {
    let ruby = Ruby::get_with(value);

    if value.is_nil() {
        return Ok(LoroValue::Null);
    }
    if value.is_kind_of(ruby.class_true_class()) {
        return Ok(LoroValue::Bool(true));
    }
    if value.is_kind_of(ruby.class_false_class()) {
        return Ok(LoroValue::Bool(false));
    }
    if value.is_kind_of(ruby.class_integer()) {
        return Ok(LoroValue::I64(i64::try_convert(value)?));
    }
    if value.is_kind_of(ruby.class_float()) {
        return Ok(LoroValue::Double(f64::try_convert(value)?));
    }
    if let Some(string) = RString::from_value(value) {
        if string.enc_get() == ruby.ascii8bit_encindex() {
            let bytes = unsafe { string.as_slice().to_vec() };
            return Ok(LoroValue::Binary(bytes.into()));
        }
        return Ok(LoroValue::String(string.to_string()?.into()));
    }
    if let Some(symbol) = Symbol::from_value(value) {
        return Ok(LoroValue::String(symbol.name()?.into_owned().into()));
    }
    if let Some(array) = RArray::from_value(value) {
        mark_seen(seen, value)?;
        let values = unsafe { array.as_slice().to_vec() };
        let converted = values
            .into_iter()
            .map(|item| ruby_to_loro_with_seen(item, seen))
            .collect::<Result<Vec<_>, _>>()?;
        seen.delete::<_, Value>(value)?;
        return Ok(LoroValue::List(converted.into()));
    }
    if let Some(hash) = RHash::from_value(value) {
        mark_seen(seen, value)?;
        let mut converted = Vec::with_capacity(hash.len());
        hash.foreach(|key: Value, value: Value| {
            converted.push((key_to_string(key)?, ruby_to_loro_with_seen(value, seen)?));
            Ok(ForEach::Continue)
        })?;
        seen.delete::<_, Value>(value)?;
        return Ok(LoroValue::Map(converted.into()));
    }

    Err(error::value_type(format!(
        "cannot convert {} to a Loro value",
        unsafe { value.classname() }
    )))
}

fn mark_seen(seen: RHash, value: Value) -> Result<(), Error> {
    let already_seen: Option<bool> = seen.lookup(value)?;
    if already_seen.is_some() {
        return Err(error::value_type(
            "cannot convert cyclic Array or Hash to a Loro value",
        ));
    }

    seen.aset(value, true)
}

pub fn loro_to_ruby(value: &LoroValue) -> Result<Value, Error> {
    let ruby = Ruby::get().unwrap();
    match value {
        LoroValue::Null => Ok(ruby.qnil().as_value()),
        LoroValue::Bool(value) => Ok(value.into_value_with(&ruby)),
        LoroValue::Double(value) => Ok(value.into_value_with(&ruby)),
        LoroValue::I64(value) => Ok(value.into_value_with(&ruby)),
        LoroValue::Binary(value) => Ok(ruby.str_from_slice(value).as_value()),
        LoroValue::String(value) => Ok(ruby.str_new(value).as_value()),
        LoroValue::List(value) => {
            let array = ruby.ary_new_capa(value.len());
            for item in value.iter() {
                array.push(loro_to_ruby(item)?)?;
            }
            Ok(array.as_value())
        }
        LoroValue::Map(value) => {
            let hash = ruby.hash_new_capa(value.len());
            for (key, item) in value.iter() {
                hash.aset(ruby.str_new(key), loro_to_ruby(item)?)?;
            }
            Ok(hash.as_value())
        }
        LoroValue::Container(id) => Err(error::loro(format!(
            "unexpected container marker in deep value: {id:?}"
        ))),
    }
}

pub fn value_or_container_to_ruby(value: &ValueOrContainer) -> Result<Value, Error> {
    loro_to_ruby(&value.get_deep_value())
}

pub fn loro_map_to_ruby(value: &LoroValue) -> Result<RHash, Error> {
    RHash::try_convert(loro_to_ruby(value)?)
}

pub fn binary_string(bytes: &[u8]) -> RString {
    Ruby::get().unwrap().str_from_slice(bytes)
}
