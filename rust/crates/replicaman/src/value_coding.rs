//! Direct serde bridge over the `ReplicaValue` tree — the Rust transliteration
//! of `Sources/ReplicaMan/ReplicaValueCoding.swift`.
//!
//! The generated models used to round-trip through JSON bytes on EVERY typed
//! read. These coders walk the tree in place: zero bytes, zero parsing.
//!
//! Contract parity with the JSON bridge it replaces:
//! - Decoding accepts BOTH the payload's own key and its snake_case variant
//!   (row data is camelized at capture, doc/catalog payloads may carry
//!   snake_case). In Rust the "own key" is the serde field name — which for a
//!   generated model is the camelCase WIRE name carried by `#[serde(rename)]`,
//!   so the fallback does the same work it does upstream.
//! - Encoding emits field names AS IS (no snake conversion on the way out),
//!   and an absent optional is OMITTED, exactly as Swift's synthesized
//!   `encodeIfPresent` does. An explicit unit/`ReplicaValue::Null` still
//!   writes `null` — the two are distinguishable here, as upstream.
//! - Integers decode from a number only when exactly representable
//!   (JSONDecoder rejects 3.5 for Int; so do we), and `f32` rejects
//!   non-finite results (1e40 was a decode failure through JSON; it must not
//!   silently become `inf`).

use std::collections::HashMap;
use std::fmt;

use parking_lot::Mutex;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use serde::{Deserializer, Serialize, Serializer, ser};

use crate::value::{ReplicaFields, ReplicaValue};

/// Anything the tree coders can refuse. Mirrors `DecodingError`/`EncodingError`
/// at the granularity callers actually branch on: they don't.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodingError(pub String);

impl fmt::Display for CodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CodingError {}

impl de::Error for CodingError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

impl ser::Error for CodingError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

type CodingResult<T> = Result<T, CodingError>;

/// Decode a `Deserialize` straight off a `ReplicaValue` tree.
pub fn decode<T: DeserializeOwned>(value: &ReplicaValue) -> CodingResult<T> {
    T::deserialize(ValueDeserializer { value })
}

/// Encode a `Serialize` into a `ReplicaValue` tree.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> CodingResult<ReplicaValue> {
    Ok(value
        .serialize(ValueSerializer)?
        .unwrap_or(ReplicaValue::Null))
}

// MARK: - Key conversion

static SNAKE_MEMO: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

/// camelCase → snake_case, mirroring Foundation's `convertToSnakeCase` far
/// enough for identifier-shaped keys ("animationInId" → "animation_in_id").
///
/// Memoized: keys are a small closed set of field names, and this runs on
/// every keyed-lookup MISS.
pub fn snake_cased(key: &str) -> String {
    {
        let memo = SNAKE_MEMO.lock();
        if let Some(hit) = memo.as_ref().and_then(|map| map.get(key)) {
            return hit.clone();
        }
    }
    let mut out = String::with_capacity(key.len() + 4);
    for character in key.chars() {
        if character.is_uppercase() {
            out.push('_');
            out.extend(character.to_lowercase());
        } else {
            out.push(character);
        }
    }
    SNAKE_MEMO
        .lock()
        .get_or_insert_with(HashMap::new)
        .insert(key.to_owned(), out.clone());
    out
}

// MARK: - Deserializer

struct ValueDeserializer<'a> {
    value: &'a ReplicaValue,
}

fn type_mismatch<T>(expected: &str, got: &ReplicaValue) -> CodingResult<T> {
    Err(CodingError(format!("expected {expected}, got {got:?}")))
}

/// A number is an integer only when it is exactly representable — the JSON
/// bridge refused 3.5 for `Int` and so do we.
fn exact_integer<T>(value: &ReplicaValue) -> CodingResult<T>
where
    T: TryFrom<i128>,
{
    if let ReplicaValue::Integer(integer) = value {
        return T::try_from(i128::from(*integer))
            .map_err(|_| CodingError("Integer out of range".into()));
    }
    let ReplicaValue::Number(number) = value else {
        return type_mismatch("an exact integer", value);
    };
    if !number.is_finite() || number.trunc() != *number {
        return type_mismatch("an exact integer", value);
    }
    // f64 carries every i53 exactly; wider values round, so go through i128
    // and confirm the round trip.
    let integer = *number as i128;
    if integer as f64 != *number {
        return type_mismatch("an exact integer", value);
    }
    T::try_from(integer)
        .map_err(|_| CodingError(format!("expected an exact integer, got {value:?}")))
}

macro_rules! deserialize_exact_integer {
    ($method:ident, $visit:ident, $ty:ty) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
            visitor.$visit(exact_integer::<$ty>(self.value)?)
        }
    };
}

impl<'de> Deserializer<'de> for ValueDeserializer<'_> {
    type Error = CodingError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::Null => visitor.visit_unit(),
            ReplicaValue::Bool(value) => visitor.visit_bool(*value),
            ReplicaValue::Number(value) => visitor.visit_f64(*value),
            ReplicaValue::Integer(value) => visitor.visit_i64(*value),
            ReplicaValue::String(value) => visitor.visit_str(value),
            ReplicaValue::Array(items) => visitor.visit_seq(SeqDeserializer { items, index: 0 }),
            ReplicaValue::Object(fields) => visitor.visit_map(RawMapDeserializer {
                entries: fields.iter().collect(),
                index: 0,
                value: None,
            }),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::Bool(value) => visitor.visit_bool(*value),
            other => type_mismatch("Bool", other),
        }
    }

    deserialize_exact_integer!(deserialize_i8, visit_i8, i8);
    deserialize_exact_integer!(deserialize_i16, visit_i16, i16);
    deserialize_exact_integer!(deserialize_i32, visit_i32, i32);
    deserialize_exact_integer!(deserialize_i64, visit_i64, i64);
    deserialize_exact_integer!(deserialize_u8, visit_u8, u8);
    deserialize_exact_integer!(deserialize_u16, visit_u16, u16);
    deserialize_exact_integer!(deserialize_u32, visit_u32, u32);
    deserialize_exact_integer!(deserialize_u64, visit_u64, u64);
    deserialize_exact_integer!(deserialize_i128, visit_i128, i128);
    deserialize_exact_integer!(deserialize_u128, visit_u128, u128);

    /// A double that overflows `f32` is a decode FAILURE, not `inf` — the JSON
    /// bridge this replaces rejected it.
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::Integer(value) => visitor.visit_f32(*value as f32),
            ReplicaValue::Number(number) => {
                let narrowed = *number as f32;
                if narrowed.is_finite() {
                    visitor.visit_f32(narrowed)
                } else {
                    type_mismatch("a finite f32", self.value)
                }
            }
            other => type_mismatch("a finite f32", other),
        }
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::Number(number) => visitor.visit_f64(*number),
            ReplicaValue::Integer(value) => visitor.visit_f64(*value as f64),
            other => type_mismatch("Double", other),
        }
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::String(value) => visitor.visit_str(value),
            other => type_mismatch("String", other),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> CodingResult<V::Value> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> CodingResult<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::Array(items) => visitor.visit_seq(SeqDeserializer { items, index: 0 }),
            other => type_mismatch("an array", other),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> CodingResult<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> CodingResult<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::Object(fields) => visitor.visit_map(RawMapDeserializer {
                entries: fields.iter().collect(),
                index: 0,
                value: None,
            }),
            other => type_mismatch("an object", other),
        }
    }

    /// The keyed container: the payload's own key wins, its snake_case
    /// spelling answers second, and a key present under neither spelling is
    /// simply absent (an optional field decodes to `None`, a required one
    /// reports the missing field).
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> CodingResult<V::Value> {
        let ReplicaValue::Object(object) = self.value else {
            return type_mismatch("an object", self.value);
        };
        let mut entries: Vec<(String, &ReplicaValue)> = Vec::with_capacity(fields.len());
        for field in fields {
            let hit = object
                .get(*field)
                .or_else(|| object.get(&snake_cased(field)));
            if let Some(value) = hit {
                entries.push(((*field).to_owned(), value));
            }
        }
        visitor.visit_map(FieldMapDeserializer {
            entries,
            index: 0,
            value: None,
        })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> CodingResult<V::Value> {
        match self.value {
            ReplicaValue::String(variant) => visitor.visit_enum(UnitVariantAccess { variant }),
            ReplicaValue::Object(fields) if fields.len() == 1 => {
                let (variant, value) = fields.iter().next().expect("checked len");
                visitor.visit_enum(TaggedVariantAccess { variant, value })
            }
            other => type_mismatch("an enum", other),
        }
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> CodingResult<V::Value> {
        visitor.visit_unit()
    }
}

struct SeqDeserializer<'a> {
    items: &'a [ReplicaValue],
    index: usize,
}

impl<'de> SeqAccess<'de> for SeqDeserializer<'_> {
    type Error = CodingError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> CodingResult<Option<T::Value>> {
        let Some(value) = self.items.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        seed.deserialize(ValueDeserializer { value }).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.items.len() - self.index)
    }
}

/// Every entry of the object, verbatim — what a `HashMap`/`BTreeMap` field or
/// an untyped map decode sees.
struct RawMapDeserializer<'a> {
    entries: Vec<(&'a String, &'a ReplicaValue)>,
    index: usize,
    value: Option<&'a ReplicaValue>,
}

impl<'de> MapAccess<'de> for RawMapDeserializer<'_> {
    type Error = CodingError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> CodingResult<Option<K::Value>> {
        let Some((key, value)) = self.entries.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        self.value = Some(value);
        seed.deserialize(key.as_str().into_deserializer()).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> CodingResult<V::Value> {
        let value = self.value.take().expect("value follows key");
        seed.deserialize(ValueDeserializer { value })
    }
}

/// The struct container: pre-resolved (field name, value) pairs, so the
/// snake_case fallback runs once per field rather than per payload key.
struct FieldMapDeserializer<'a> {
    entries: Vec<(String, &'a ReplicaValue)>,
    index: usize,
    value: Option<&'a ReplicaValue>,
}

impl<'de> MapAccess<'de> for FieldMapDeserializer<'_> {
    type Error = CodingError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> CodingResult<Option<K::Value>> {
        let Some((key, value)) = self.entries.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        self.value = Some(value);
        seed.deserialize(key.as_str().into_deserializer()).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> CodingResult<V::Value> {
        let value = self.value.take().expect("value follows key");
        seed.deserialize(ValueDeserializer { value })
    }
}

struct UnitVariantAccess<'a> {
    variant: &'a str,
}

impl<'de> EnumAccess<'de> for UnitVariantAccess<'_> {
    type Error = CodingError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> CodingResult<(V::Value, Self)> {
        let value = seed.deserialize(self.variant.into_deserializer())?;
        Ok((value, self))
    }
}

impl<'de> VariantAccess<'de> for UnitVariantAccess<'_> {
    type Error = CodingError;

    fn unit_variant(self) -> CodingResult<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, _seed: T) -> CodingResult<T::Value> {
        Err(CodingError("expected a keyed variant".into()))
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, _visitor: V) -> CodingResult<V::Value> {
        Err(CodingError("expected a keyed variant".into()))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> CodingResult<V::Value> {
        Err(CodingError("expected a keyed variant".into()))
    }
}

struct TaggedVariantAccess<'a> {
    variant: &'a str,
    value: &'a ReplicaValue,
}

impl<'de> EnumAccess<'de> for TaggedVariantAccess<'_> {
    type Error = CodingError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> CodingResult<(V::Value, Self)> {
        let value = seed.deserialize(self.variant.into_deserializer())?;
        Ok((value, self))
    }
}

impl<'de> VariantAccess<'de> for TaggedVariantAccess<'_> {
    type Error = CodingError;

    fn unit_variant(self) -> CodingResult<()> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> CodingResult<T::Value> {
        seed.deserialize(ValueDeserializer { value: self.value })
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> CodingResult<V::Value> {
        ValueDeserializer { value: self.value }.deserialize_seq(visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> CodingResult<V::Value> {
        ValueDeserializer { value: self.value }.deserialize_struct("", fields, visitor)
    }
}

// MARK: - Serializer
//
// `Ok = Option<ReplicaValue>`: `None` is "this value is ABSENT", which is what
// `Option::None` produces and what a struct field then omits — Swift's
// `encodeIfPresent`. An explicit unit still yields `Some(Null)`, so a
// deliberately-null field survives.

type Encoded = Option<ReplicaValue>;

struct ValueSerializer;

macro_rules! serialize_number {
    ($method:ident, $ty:ty) => {
        fn $method(self, value: $ty) -> CodingResult<Encoded> {
            let integer = i64::try_from(value)
                .map_err(|_| CodingError("Integer outside signed 64-bit range".into()))?;
            Ok(Some(ReplicaValue::signed_integer(integer)))
        }
    };
}

impl Serializer for ValueSerializer {
    type Ok = Encoded;
    type Error = CodingError;
    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = SeqSerializer;
    type SerializeTupleVariant = TupleVariantSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeStructVariant = StructVariantSerializer;

    fn serialize_bool(self, value: bool) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Bool(value)))
    }

    serialize_number!(serialize_i8, i8);
    serialize_number!(serialize_i16, i16);
    serialize_number!(serialize_i32, i32);
    serialize_number!(serialize_i64, i64);
    serialize_number!(serialize_u8, u8);
    serialize_number!(serialize_u16, u16);
    serialize_number!(serialize_u32, u32);
    serialize_number!(serialize_u64, u64);
    serialize_number!(serialize_i128, i128);
    serialize_number!(serialize_u128, u128);
    fn serialize_f32(self, value: f32) -> CodingResult<Encoded> {
        self.serialize_f64(f64::from(value))
    }
    fn serialize_f64(self, value: f64) -> CodingResult<Encoded> {
        if !value.is_finite() {
            return Err(CodingError("Nonfinite JSON number".into()));
        }
        Ok(Some(ReplicaValue::Number(value)))
    }

    fn serialize_char(self, value: char) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::String(value.to_string())))
    }

    fn serialize_str(self, value: &str) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::String(value.to_owned())))
    }

    fn serialize_bytes(self, value: &[u8]) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Array(
            value
                .iter()
                .map(|byte| ReplicaValue::Number(f64::from(*byte)))
                .collect(),
        )))
    }

    /// ABSENT, not null — a struct drops the field the way `encodeIfPresent`
    /// does.
    fn serialize_none(self) -> CodingResult<Encoded> {
        Ok(None)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> CodingResult<Encoded> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Null))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Null))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::String(variant.to_owned())))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> CodingResult<Encoded> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> CodingResult<Encoded> {
        let mut object = ReplicaFields::new();
        object.insert(
            variant.to_owned(),
            value
                .serialize(ValueSerializer)?
                .unwrap_or(ReplicaValue::Null),
        );
        Ok(Some(ReplicaValue::Object(object)))
    }

    fn serialize_seq(self, _len: Option<usize>) -> CodingResult<SeqSerializer> {
        Ok(SeqSerializer { items: Vec::new() })
    }

    fn serialize_tuple(self, _len: usize) -> CodingResult<SeqSerializer> {
        Ok(SeqSerializer { items: Vec::new() })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> CodingResult<SeqSerializer> {
        Ok(SeqSerializer { items: Vec::new() })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> CodingResult<TupleVariantSerializer> {
        Ok(TupleVariantSerializer {
            variant,
            items: Vec::new(),
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> CodingResult<MapSerializer> {
        Ok(MapSerializer {
            object: ReplicaFields::new(),
            key: None,
        })
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> CodingResult<StructSerializer> {
        Ok(StructSerializer {
            object: ReplicaFields::new(),
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> CodingResult<StructVariantSerializer> {
        Ok(StructVariantSerializer {
            variant,
            object: ReplicaFields::new(),
        })
    }
}

struct SeqSerializer {
    items: Vec<ReplicaValue>,
}

impl SeqSerializer {
    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> CodingResult<()> {
        // An unkeyed container materializes an absent value as `null`, the
        // way Swift's `encodeNil()` does.
        self.items.push(
            value
                .serialize(ValueSerializer)?
                .unwrap_or(ReplicaValue::Null),
        );
        Ok(())
    }
}

impl ser::SerializeSeq for SeqSerializer {
    type Ok = Encoded;
    type Error = CodingError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> CodingResult<()> {
        self.push(value)
    }

    fn end(self) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Array(self.items)))
    }
}

impl ser::SerializeTuple for SeqSerializer {
    type Ok = Encoded;
    type Error = CodingError;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> CodingResult<()> {
        self.push(value)
    }

    fn end(self) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Array(self.items)))
    }
}

impl ser::SerializeTupleStruct for SeqSerializer {
    type Ok = Encoded;
    type Error = CodingError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> CodingResult<()> {
        self.push(value)
    }

    fn end(self) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Array(self.items)))
    }
}

struct TupleVariantSerializer {
    variant: &'static str,
    items: Vec<ReplicaValue>,
}

impl ser::SerializeTupleVariant for TupleVariantSerializer {
    type Ok = Encoded;
    type Error = CodingError;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> CodingResult<()> {
        self.items.push(
            value
                .serialize(ValueSerializer)?
                .unwrap_or(ReplicaValue::Null),
        );
        Ok(())
    }

    fn end(self) -> CodingResult<Encoded> {
        let mut object = ReplicaFields::new();
        object.insert(self.variant.to_owned(), ReplicaValue::Array(self.items));
        Ok(Some(ReplicaValue::Object(object)))
    }
}

struct MapSerializer {
    object: ReplicaFields,
    key: Option<String>,
}

impl ser::SerializeMap for MapSerializer {
    type Ok = Encoded;
    type Error = CodingError;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> CodingResult<()> {
        match key.serialize(ValueSerializer)? {
            Some(ReplicaValue::String(text)) => {
                self.key = Some(text);
                Ok(())
            }
            other => Err(CodingError(format!(
                "map keys must be strings, got {other:?}"
            ))),
        }
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> CodingResult<()> {
        let key = self.key.take().expect("value follows key");
        // A map is an explicit dictionary: a `None` value is a stored null,
        // not an omission.
        self.object.insert(
            key,
            value
                .serialize(ValueSerializer)?
                .unwrap_or(ReplicaValue::Null),
        );
        Ok(())
    }

    fn end(self) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Object(self.object)))
    }
}

struct StructSerializer {
    object: ReplicaFields,
}

impl ser::SerializeStruct for StructSerializer {
    type Ok = Encoded;
    type Error = CodingError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> CodingResult<()> {
        // Synthesized Codable skips nils; the tree must too.
        if let Some(encoded) = value.serialize(ValueSerializer)? {
            self.object.insert(key.to_owned(), encoded);
        }
        Ok(())
    }

    fn skip_field(&mut self, _key: &'static str) -> CodingResult<()> {
        Ok(())
    }

    fn end(self) -> CodingResult<Encoded> {
        Ok(Some(ReplicaValue::Object(self.object)))
    }
}

struct StructVariantSerializer {
    variant: &'static str,
    object: ReplicaFields,
}

impl ser::SerializeStructVariant for StructVariantSerializer {
    type Ok = Encoded;
    type Error = CodingError;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> CodingResult<()> {
        if let Some(encoded) = value.serialize(ValueSerializer)? {
            self.object.insert(key.to_owned(), encoded);
        }
        Ok(())
    }

    fn skip_field(&mut self, _key: &'static str) -> CodingResult<()> {
        Ok(())
    }

    fn end(self) -> CodingResult<Encoded> {
        let mut outer = ReplicaFields::new();
        outer.insert(self.variant.to_owned(), ReplicaValue::Object(self.object));
        Ok(Some(ReplicaValue::Object(outer)))
    }
}
