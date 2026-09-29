use crate::{ReplicaError, ReplicaFields, ReplicaResult, ReplicaValue};

/// A relationship whose target lifetime travels with each mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaReferenceSpec {
    pub name: String,
    pub stream: String,
    pub field: Option<String>,
    pub key_segment: Option<usize>,
    pub key_prefix: Option<String>,
    pub optional: bool,
}

impl ReplicaReferenceSpec {
    pub fn field(
        name: impl Into<String>,
        stream: impl Into<String>,
        field: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            stream: stream.into(),
            field: Some(field.into()),
            key_segment: None,
            key_prefix: None,
            optional: false,
        }
    }

    pub(crate) fn target(
        &self,
        row_id: &str,
        data: &ReplicaFields,
    ) -> ReplicaResult<Option<String>> {
        if self
            .key_prefix
            .as_ref()
            .is_some_and(|prefix| !row_id.starts_with(prefix))
        {
            return Ok(None);
        }
        let id = if let Some(segment) = self.key_segment {
            row_id.split('/').nth(segment)
        } else {
            let value = data.get(self.field.as_ref().unwrap_or(&self.name));
            if self.optional && (value.is_none() || value == Some(&ReplicaValue::Null)) {
                return Ok(None);
            }
            value.and_then(ReplicaValue::as_string)
        };
        let id = id.filter(|id| !id.is_empty()).ok_or_else(|| {
            ReplicaError::Storage(format!("Missing or invalid reference: {}", self.name))
        })?;
        Ok(Some(id.to_owned()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaReference {
    pub name: String,
    pub stream: String,
    pub id: String,
    pub incarnation: String,
}

impl ReplicaReference {
    pub(crate) fn to_value(&self) -> ReplicaValue {
        ReplicaValue::Object(
            [
                ("name".into(), ReplicaValue::string(&self.name)),
                ("stream".into(), ReplicaValue::string(&self.stream)),
                ("id".into(), ReplicaValue::string(&self.id)),
                (
                    "incarnation".into(),
                    ReplicaValue::string(&self.incarnation),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }

    pub(crate) fn from_value(value: &ReplicaValue) -> ReplicaResult<Self> {
        let text = |key: &str| {
            value
                .get(key)
                .and_then(ReplicaValue::as_string)
                .filter(|text| !text.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    ReplicaError::Storage(format!("Reference {key} must be a nonempty string"))
                })
        };
        Ok(Self {
            name: text("name")?,
            stream: text("stream")?,
            id: text("id")?,
            incarnation: text("incarnation")?,
        })
    }
}

pub(crate) fn derived_incarnation(
    namespace: &str,
    stream: &str,
    id: &str,
    parent: &ReplicaReference,
) -> String {
    let parts = [
        "replicaman:derived:1",
        namespace,
        stream,
        id,
        &parent.stream,
        &parent.id,
        &parent.incarnation,
    ];
    let content: String = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect();
    format!("derived:{}", crate::protocol::digest(content.as_bytes()))
}
