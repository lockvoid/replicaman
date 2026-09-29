//! Direct `serde` bridge over the `DocumentValue` tree — the doc-lane twin of
//! ReplicaMan's row-lane `ReplicaValueCoding`.
//!
//! The generated document models used to round-trip through JSON on EVERY
//! typed read (`DocumentFields` → JSON bytes → typed struct); the
//! hang-sampler stacks caught `TextStyle` decoding grinding through that
//! double serialization on the main thread. This decoder walks the tree in
//! place: zero bytes, zero parsing.
//!
//! DECODE-ONLY by design (LOCKED ruling #9): encode stays on the shaped-JSON
//! path, because the manifest `Shape` descriptor drives `.int` vs `.double` on
//! the way OUT, which a blind serializer cannot know.
//!
//! Contract parity with the JSON bridge it replaces:
//! - Keyed lookup accepts the payload's own key and its snake_case variant
//!   (the old bridge ran `convertFromSnakeCase`; doc keys are snake_case).
//! - `Int(i64)` and `Double(f64)` stay distinct: integers decode from `Int`
//!   exactly, and from `Double` only when exactly representable (JSON had no
//!   int/double distinction, so `5.0` decoded as `5` — that stands); `f64`
//!   decodes from both; `f32` additionally rejects non-finite results.

use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use parking_lot::Mutex;
use serde::de::value::StrDeserializer;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, IntoDeserializer, MapAccess,
    SeqAccess, VariantAccess, Visitor,
};

use crate::document_value::{DocumentFields, DocumentValue};

/// Decode a `Deserialize` type straight off a document field bag.
pub fn decode<T: DeserializeOwned>(fields: &DocumentFields) -> Result<T, DecodeError> {
    let value = DocumentValue::Map(fields.clone());
    T::deserialize(ValueDecoder::root(&value))
}

/// camelCase → snake_case, far enough for identifier-shaped keys
/// ("animationInId" → "animation_in_id").
///
/// Memoized: keys are serde field names — a small closed set — and this runs on
/// every keyed-lookup MISS, where the per-scalar uppercase walk was a
/// hang-sampler stall on iOS (225 ms of `TextStyle` decode inside one open).
pub(crate) fn snake_cased(key: &str) -> String {
    static MEMO: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

    {
        let memo = MEMO.lock();
        if let Some(hit) = memo.as_ref().and_then(|memo| memo.get(key)) {
            return hit.clone();
        }
    }

    let mut out = String::with_capacity(key.len() + 4);
    for scalar in key.chars() {
        if scalar.is_uppercase() {
            out.push('_');
            out.extend(scalar.to_lowercase());
        } else {
            out.push(scalar);
        }
    }

    MEMO.lock()
        .get_or_insert_with(HashMap::new)
        .insert(key.to_owned(), out.clone());

    out
}

// MARK: - Error

/// The refusals the Swift bridge spells as `DecodingError`, kept apart so a
/// caller can tell a missing key from a wrong type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The value at `path` is not the kind the type asked for.
    TypeMismatch { path: String, expected: String },
    /// A required key is absent from the field bag.
    KeyNotFound { path: String, key: String },
    /// A container ran out of values before the type was satisfied.
    ValueNotFound { path: String },
    /// Anything `serde` itself reports.
    Custom(String),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::TypeMismatch { path, expected } => {
                write!(formatter, "{path}: expected {expected}")
            }
            DecodeError::KeyNotFound { path, key } => {
                write!(formatter, "{path}: no value for {key}")
            }
            DecodeError::ValueNotFound { path } => {
                write!(formatter, "{path}: container exhausted")
            }
            DecodeError::Custom(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for DecodeError {}

impl de::Error for DecodeError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        DecodeError::Custom(message.to_string())
    }
}

// MARK: - Decoder

/// The dotted path of the value being decoded, kept for error messages only —
/// the `Rc` is what keeps descending cheap.
type Path = Rc<str>;

fn child_path(parent: &Path, segment: &str) -> Path {
    if parent.is_empty() {
        Rc::from(segment)
    } else {
        Rc::from(format!("{parent}.{segment}"))
    }
}

struct ValueDecoder<'a> {
    value: &'a DocumentValue,
    path: Path,
}

impl<'a> ValueDecoder<'a> {
    fn root(value: &'a DocumentValue) -> Self {
        Self {
            value,
            path: Rc::from(""),
        }
    }

    fn at(value: &'a DocumentValue, path: Path) -> Self {
        Self { value, path }
    }

    fn mismatch(&self, expected: &str) -> DecodeError {
        DecodeError::TypeMismatch {
            path: self.path.to_string(),
            expected: format!("{expected}, got {}", kind_of(self.value)),
        }
    }

    fn exact_int<T: TryFrom<i128>>(&self) -> Result<T, DecodeError> {
        let exact = match self.value {
            DocumentValue::Int(number) => T::try_from(i128::from(*number)).ok(),
            DocumentValue::Double(number) => {
                exact_i128(*number).and_then(|number| T::try_from(number).ok())
            }
            _ => None,
        };

        exact.ok_or_else(|| self.mismatch(&format!("exact {}", std::any::type_name::<T>())))
    }

    fn double(&self) -> Result<f64, DecodeError> {
        self.value
            .double_value()
            .ok_or_else(|| self.mismatch("a double"))
    }

    /// A double that overflows `f32` is a decode FAILURE, not `inf` — the JSON
    /// bridge this replaces rejected it.
    fn float(&self) -> Result<f32, DecodeError> {
        let float = self.double()? as f32;
        if float.is_finite() {
            Ok(float)
        } else {
            Err(self.mismatch("a finite f32"))
        }
    }
}

/// `T(exactly:)` for a `Double`: whole, finite, and inside the integer domain.
fn exact_i128(number: f64) -> Option<i128> {
    if !number.is_finite() || number.fract() != 0.0 {
        return None;
    }
    if number < -(2.0_f64.powi(127)) || number >= 2.0_f64.powi(127) {
        return None;
    }
    Some(number as i128)
}

fn kind_of(value: &DocumentValue) -> &'static str {
    match value {
        DocumentValue::Null => "null",
        DocumentValue::Bool(_) => "a bool",
        DocumentValue::Int(_) => "an int",
        DocumentValue::Double(_) => "a double",
        DocumentValue::String(_) => "a string",
        DocumentValue::List(_) => "a list",
        DocumentValue::Map(_) => "a map",
    }
}

macro_rules! deserialize_integer {
    ($method:ident, $visit:ident, $ty:ty) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
            let number = self.exact_int::<$ty>()?;
            visitor.$visit(number)
        }
    };
}

impl<'de> Deserializer<'de> for ValueDecoder<'_> {
    type Error = DecodeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        match self.value {
            DocumentValue::Null => visitor.visit_unit(),
            DocumentValue::Bool(value) => visitor.visit_bool(*value),
            DocumentValue::Int(value) => visitor.visit_i64(*value),
            DocumentValue::Double(value) => visitor.visit_f64(*value),
            DocumentValue::String(value) => visitor.visit_str(value),
            DocumentValue::List(_) => self.deserialize_seq(visitor),
            DocumentValue::Map(_) => self.deserialize_map(visitor),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        let value = self
            .value
            .bool_value()
            .ok_or_else(|| self.mismatch("a bool"))?;
        visitor.visit_bool(value)
    }

    deserialize_integer!(deserialize_i8, visit_i8, i8);
    deserialize_integer!(deserialize_i16, visit_i16, i16);
    deserialize_integer!(deserialize_i32, visit_i32, i32);
    deserialize_integer!(deserialize_i64, visit_i64, i64);
    deserialize_integer!(deserialize_i128, visit_i128, i128);
    deserialize_integer!(deserialize_u8, visit_u8, u8);
    deserialize_integer!(deserialize_u16, visit_u16, u16);
    deserialize_integer!(deserialize_u32, visit_u32, u32);
    deserialize_integer!(deserialize_u64, visit_u64, u64);
    deserialize_integer!(deserialize_u128, visit_u128, u128);

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        let value = self.float()?;
        visitor.visit_f32(value)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        let value = self.double()?;
        visitor.visit_f64(value)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        match self.value.string_value().and_then(|text| {
            let mut chars = text.chars();
            chars.next().filter(|_| chars.next().is_none())
        }) {
            Some(value) => visitor.visit_char(value),
            None => Err(self.mismatch("a single character")),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        let value = self
            .value
            .string_value()
            .ok_or_else(|| self.mismatch("a string"))?;
        visitor.visit_str(value)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_str(visitor)
    }

    /// Nothing in the timeline is bytes — the `DocumentValue` vocabulary has no
    /// binary case at all, so this is always a refusal.
    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, DecodeError> {
        Err(self.mismatch("bytes, which a document never holds"))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        match self.value {
            DocumentValue::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        match self.value {
            DocumentValue::Null => visitor.visit_unit(),
            _ => Err(self.mismatch("null")),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        let DocumentValue::List(list) = self.value else {
            return Err(self.mismatch("a list"));
        };
        visitor.visit_seq(ListAccess {
            list,
            index: 0,
            path: self.path.clone(),
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        let DocumentValue::Map(map) = self.value else {
            return Err(self.mismatch("a map"));
        };
        visitor.visit_map(BagAccess {
            entries: map.iter().collect(),
            index: 0,
            path: self.path.clone(),
        })
    }

    /// The old bridge's `convertFromSnakeCase` equivalence, in serde's
    /// iteration-shaped world: rather than yielding whatever keys the payload
    /// happens to carry, resolve each DECLARED field — its own spelling first,
    /// its snake_case spelling second — and yield only what resolved. Absent
    /// fields simply never appear, which is how `Option` lands as `None` and a
    /// required field reports itself missing; unknown payload keys are ignored,
    /// which is document-evolution rule 2.
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        let DocumentValue::Map(map) = self.value else {
            return Err(self.mismatch("a map"));
        };

        let entries = fields
            .iter()
            .filter_map(|field| lookup(map, field).map(|value| (*field, value)))
            .collect();

        visitor.visit_map(FieldAccess {
            entries,
            index: 0,
            path: self.path.clone(),
        })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        match self.value {
            DocumentValue::String(variant) => visitor.visit_enum(UnitVariant { variant }),
            DocumentValue::Map(map) if map.len() == 1 => {
                let (variant, value) = map.iter().next().expect("len == 1");
                visitor.visit_enum(TaggedVariant {
                    variant,
                    value,
                    path: child_path(&self.path, variant),
                })
            }
            _ => Err(self.mismatch("an enum")),
        }
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        visitor.visit_unit()
    }
}

/// The payload's own key wins; its snake_case spelling answers second.
fn lookup<'a>(map: &'a DocumentFields, field: &str) -> Option<&'a DocumentValue> {
    map.get(field)
        .or_else(|| map.get(snake_cased(field).as_str()))
}

// MARK: - Containers

struct FieldAccess<'a> {
    entries: Vec<(&'static str, &'a DocumentValue)>,
    index: usize,
    path: Path,
}

impl<'de> MapAccess<'de> for FieldAccess<'_> {
    type Error = DecodeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, DecodeError> {
        match self.entries.get(self.index) {
            None => Ok(None),
            Some((field, _)) => {
                let key: StrDeserializer<'_, DecodeError> = (*field).into_deserializer();
                seed.deserialize(key).map(Some)
            }
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, DecodeError> {
        let (field, value) = self.entries[self.index];
        self.index += 1;
        seed.deserialize(ValueDecoder::at(value, child_path(&self.path, field)))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len() - self.index)
    }
}

struct BagAccess<'a> {
    entries: Vec<(&'a String, &'a DocumentValue)>,
    index: usize,
    path: Path,
}

impl<'de> MapAccess<'de> for BagAccess<'_> {
    type Error = DecodeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, DecodeError> {
        match self.entries.get(self.index) {
            None => Ok(None),
            Some((key, _)) => {
                let key: StrDeserializer<'_, DecodeError> = key.as_str().into_deserializer();
                seed.deserialize(key).map(Some)
            }
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, DecodeError> {
        let (key, value) = self.entries[self.index];
        self.index += 1;
        seed.deserialize(ValueDecoder::at(value, child_path(&self.path, key)))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len() - self.index)
    }
}

struct ListAccess<'a> {
    list: &'a [DocumentValue],
    index: usize,
    path: Path,
}

impl<'de> SeqAccess<'de> for ListAccess<'_> {
    type Error = DecodeError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, DecodeError> {
        let Some(value) = self.list.get(self.index) else {
            return Ok(None);
        };
        let path = child_path(&self.path, &format!("[{}]", self.index));
        self.index += 1;
        seed.deserialize(ValueDecoder::at(value, path)).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.list.len() - self.index)
    }
}

// MARK: - Enums

struct UnitVariant<'a> {
    variant: &'a str,
}

impl<'de> EnumAccess<'de> for UnitVariant<'_> {
    type Error = DecodeError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), DecodeError> {
        let variant: StrDeserializer<'_, DecodeError> = self.variant.into_deserializer();
        Ok((seed.deserialize(variant)?, self))
    }
}

impl<'de> VariantAccess<'de> for UnitVariant<'_> {
    type Error = DecodeError;

    fn unit_variant(self) -> Result<(), DecodeError> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        _seed: T,
    ) -> Result<T::Value, DecodeError> {
        Err(DecodeError::TypeMismatch {
            path: self.variant.to_owned(),
            expected: "a unit variant, got a newtype variant".to_owned(),
        })
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, DecodeError> {
        Err(DecodeError::TypeMismatch {
            path: self.variant.to_owned(),
            expected: "a unit variant, got a tuple variant".to_owned(),
        })
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, DecodeError> {
        Err(DecodeError::TypeMismatch {
            path: self.variant.to_owned(),
            expected: "a unit variant, got a struct variant".to_owned(),
        })
    }
}

struct TaggedVariant<'a> {
    variant: &'a str,
    value: &'a DocumentValue,
    path: Path,
}

impl<'de> EnumAccess<'de> for TaggedVariant<'_> {
    type Error = DecodeError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), DecodeError> {
        let variant: StrDeserializer<'_, DecodeError> = self.variant.into_deserializer();
        let decoded = seed.deserialize(variant)?;
        Ok((decoded, self))
    }
}

impl<'de> VariantAccess<'de> for TaggedVariant<'_> {
    type Error = DecodeError;

    fn unit_variant(self) -> Result<(), DecodeError> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, DecodeError> {
        seed.deserialize(ValueDecoder::at(self.value, self.path))
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        ValueDecoder::at(self.value, self.path).deserialize_seq(visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        ValueDecoder::at(self.value, self.path).deserialize_struct("", fields, visitor)
    }
}
