use replicaman::document_value::{DocumentEntry, DocumentFields, DocumentValue};
use replicaman_generated_contract::documents::{DeckDefaults, DeckDocument, DeckProjection};

fn default_projection() -> DeckProjection {
    DeckDocument::new(
        DeckDefaults::layouts(),
        DeckDefaults::settings(),
        Vec::new(),
    )
    .projection()
}

#[test]
fn complete_document_refuses_to_discard_an_undecodable_entry() {
    let mut projection = default_projection();
    assert!(DeckDocument::from_projection(&projection).is_some());
    projection.slides.push(DocumentEntry {
        key: "broken".into(),
        fields: DocumentFields::from([(
            "duration".into(),
            DocumentValue::String("not a number".into()),
        )]),
    });

    assert!(DeckDocument::from_projection(&projection).is_none());
}

#[test]
fn duplicate_registry_keys_are_rejected_instead_of_overwriting_an_entry() {
    let mut projection = default_projection();
    assert!(DeckDocument::from_projection(&projection).is_some());
    projection.layouts.push(projection.layouts[0].clone());
    assert!(DeckDocument::from_projection(&projection).is_none());
}
