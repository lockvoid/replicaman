mod doc;
mod error;
mod map;
mod value;

use crate::{doc::Doc, map::Map};
use magnus::{function, method, prelude::*, Error, Ruby};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = ruby.define_module("Loro")?;
    let base_error = module.define_error("Error", ruby.exception_standard_error())?;
    module.define_error("ImportError", base_error)?;
    module.define_error("TypeError", base_error)?;
    module.const_set("LORO_CRATE_VERSION", "1.13.7")?;

    let doc = module.define_class("Doc", ruby.class_object())?;
    doc.define_singleton_method("new", function!(Doc::new, -1))?;
    doc.define_singleton_method("from_snapshot", function!(Doc::from_snapshot, -1))?;
    doc.define_method("peer_id", method!(Doc::peer_id, 0))?;
    doc.define_method("peer_id=", method!(Doc::set_peer_id, 1))?;
    doc.define_method("import", method!(Doc::import, 1))?;
    doc.define_method("import_batch", method!(Doc::import_batch, 1))?;
    doc.define_method("export_updates", method!(Doc::export_updates, -1))?;
    doc.define_method("export_snapshot", method!(Doc::export_snapshot, 0))?;
    doc.define_method("version_vector", method!(Doc::version_vector, 0))?;
    doc.define_method("frontiers", method!(Doc::frontiers, 0))?;
    doc.define_method("commit", method!(Doc::commit, 0))?;
    doc.define_method("checkout", method!(Doc::checkout, 1))?;
    doc.define_method("checkout_to_latest", method!(Doc::checkout_to_latest, 0))?;
    doc.define_method("detached?", method!(Doc::is_detached, 0))?;
    doc.define_method("revert_to", method!(Doc::revert_to, 1))?;
    doc.define_method("fork_at", method!(Doc::fork_at, -1))?;
    doc.define_method("get_map", method!(Doc::get_map, 1))?;
    doc.define_method("to_h", method!(Doc::to_h, 0))?;

    let map = module.define_class("Map", ruby.class_object())?;
    map.define_method("set", method!(Map::set, 2))?;
    map.define_method("get", method!(Map::get, 1))?;
    map.define_method("get_map", method!(Map::get_map, 1))?;
    map.define_method("ensure_mergeable_map", method!(Map::ensure_mergeable_map, 1))?;
    map.define_method("delete", method!(Map::delete, 1))?;
    map.define_method("key?", method!(Map::contains_key, 1))?;
    map.define_method("keys", method!(Map::keys, 0))?;
    map.define_method("size", method!(Map::size, 0))?;
    map.define_method("to_h", method!(Map::to_h, 0))?;

    Ok(())
}
