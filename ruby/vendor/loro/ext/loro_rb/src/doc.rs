use crate::{error, map::Map, value};
use loro::{ExportMode, Frontiers, LoroDoc, VersionVector};
use magnus::{
    scan_args::{get_kwargs, scan_args},
    value::ReprValue,
    Error, RArray, RHash, RString, Ruby, TryConvert, Value,
};

#[magnus::wrap(class = "Loro::Doc", free_immediately, size)]
pub struct Doc {
    inner: LoroDoc,
}

impl Doc {
    fn configured(inner: LoroDoc, peer_id: Option<u64>) -> Result<Self, Error> {
        inner.set_record_timestamp(false);
        if let Some(peer_id) = peer_id {
            inner.set_peer_id(peer_id).map_err(error::loro)?;
        }
        Ok(Self { inner })
    }

    pub fn new(args: &[Value]) -> Result<Self, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let kwargs = get_kwargs::<_, (), (Option<Value>,), ()>(args.keywords, &[], &["peer_id"])?;
        Self::configured(LoroDoc::new(), Self::optional_peer_id(kwargs.optional.0)?)
    }

    pub fn from_snapshot(args: &[Value]) -> Result<Self, Error> {
        let args = scan_args::<(RString,), (), (), (), RHash, ()>(args)?;
        let kwargs = get_kwargs::<_, (), (Option<Value>,), ()>(args.keywords, &[], &["peer_id"])?;
        let bytes = unsafe { args.required.0.as_slice() };
        let doc = LoroDoc::from_snapshot(bytes).map_err(error::import)?;
        Self::configured(doc, Self::optional_peer_id(kwargs.optional.0)?)
    }

    fn optional_peer_id(value: Option<Value>) -> Result<Option<u64>, Error> {
        match value {
            None => Ok(None),
            Some(value) if value.is_nil() => Ok(None),
            Some(value) => u64::try_convert(value).map(Some),
        }
    }

    pub fn peer_id(&self) -> u64 {
        self.inner.peer_id()
    }

    pub fn set_peer_id(&self, peer_id: u64) -> Result<u64, Error> {
        self.inner.set_peer_id(peer_id).map_err(error::loro)?;
        Ok(peer_id)
    }

    pub fn import(&self, bytes: RString) -> Result<RHash, Error> {
        let bytes = unsafe { bytes.as_slice() };
        let status = self.inner.import(bytes).map_err(error::import)?;
        Self::import_status(status.pending.is_some())
    }

    pub fn import_batch(&self, blobs: RArray) -> Result<RHash, Error> {
        let values = unsafe { blobs.as_slice().to_vec() };
        let mut bytes = Vec::with_capacity(values.len());
        for item in values {
            let string = RString::from_value(item).ok_or_else(|| {
                Error::new(
                    Ruby::get().unwrap().exception_type_error(),
                    "import_batch entries must be Strings",
                )
            })?;
            bytes.push(unsafe { string.as_slice().to_vec() });
        }
        let status = self.inner.import_batch(&bytes).map_err(error::import)?;
        Self::import_status(status.pending.is_some())
    }

    fn import_status(pending: bool) -> Result<RHash, Error> {
        let ruby = Ruby::get().unwrap();
        let hash = ruby.hash_new_capa(1);
        hash.aset(ruby.to_symbol("pending"), pending)?;
        Ok(hash)
    }

    pub fn export_updates(&self, args: &[Value]) -> Result<RString, Error> {
        let args = scan_args::<(), (), (), (), RHash, ()>(args)?;
        let kwargs = get_kwargs::<_, (), (Option<Value>,), ()>(args.keywords, &[], &["since"])?;
        let bytes = match kwargs.optional.0 {
            Some(encoded) if !encoded.is_nil() => {
                let encoded = RString::from_value(encoded).ok_or_else(|| {
                    Error::new(
                        Ruby::get().unwrap().exception_type_error(),
                        "since must be a String or nil",
                    )
                })?;
                let encoded = unsafe { encoded.as_slice() };
                let vector = VersionVector::decode(encoded).map_err(error::loro)?;
                self.inner
                    .export(ExportMode::updates(&vector))
                    .map_err(error::loro)?
            }
            _ => self
                .inner
                .export(ExportMode::all_updates())
                .map_err(error::loro)?,
        };
        Ok(value::binary_string(&bytes))
    }

    pub fn export_snapshot(&self) -> Result<RString, Error> {
        let bytes = self
            .inner
            .export(ExportMode::Snapshot)
            .map_err(error::loro)?;
        Ok(value::binary_string(&bytes))
    }

    pub fn version_vector(&self) -> RString {
        self.inner.commit();
        let vector = self.inner.oplog_vv();
        let mut entries = vector
            .iter()
            .map(|(peer, counter)| (*peer, *counter))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(peer, _)| *peer);
        let mut canonical = VersionVector::default();
        canonical.extend(entries);
        value::binary_string(&canonical.encode())
    }

    pub fn frontiers(&self) -> RString {
        // Commit first, exactly as `version_vector` does. Under auto-commit an
        // uncommitted local edit is absent from `state_frontiers`, so a caller
        // recording "the version I just read" would record one that omits it.
        self.inner.commit();
        value::binary_string(&self.inner.state_frontiers().encode())
    }

    pub fn commit(&self) {
        self.inner.commit();
    }

    // History. A version is an encoded `Frontiers` — the bytes `frontiers`
    // answers — and the doc's full history rides its snapshot, so any past
    // state is one checkout away.

    /// A read-only view of the state at `frontiers` (detached mode).
    pub fn checkout(&self, encoded: RString) -> Result<(), Error> {
        let frontiers = Self::decode_frontiers(encoded)?;
        self.inner.checkout(&frontiers).map_err(error::loro)
    }

    pub fn checkout_to_latest(&self) {
        self.inner.checkout_to_latest();
    }

    pub fn is_detached(&self) -> bool {
        self.inner.is_detached()
    }

    /// Makes the CURRENT state equal the state at `frontiers` by appending
    /// new operations — history stays linear, every version stays reachable.
    pub fn revert_to(&self, encoded: RString) -> Result<(), Error> {
        let frontiers = Self::decode_frontiers(encoded)?;
        self.inner.revert_to(&frontiers).map_err(error::loro)
    }

    /// A new, independent doc whose history ends at `frontiers` — a branch.
    pub fn fork_at(&self, args: &[Value]) -> Result<Self, Error> {
        let args = scan_args::<(RString,), (), (), (), RHash, ()>(args)?;
        let kwargs = get_kwargs::<_, (), (Option<Value>,), ()>(args.keywords, &[], &["peer_id"])?;
        let frontiers = Self::decode_frontiers(args.required.0)?;
        let forked = self.inner.fork_at(&frontiers).map_err(error::loro)?;
        Self::configured(forked, Self::optional_peer_id(kwargs.optional.0)?)
    }

    fn decode_frontiers(encoded: RString) -> Result<Frontiers, Error> {
        let bytes = unsafe { encoded.as_slice() };
        Frontiers::decode(bytes).map_err(error::loro)
    }

    pub fn get_map(&self, name: Value) -> Result<Map, Error> {
        let name = value::key_to_string(name)?;
        Ok(Map::from_inner(self.inner.get_map(name)))
    }

    pub fn to_h(&self) -> Result<RHash, Error> {
        value::loro_map_to_ruby(&self.inner.get_deep_value())
    }
}
