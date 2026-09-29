use crate::error::{ReplicaError, ReplicaResult};
use std::fs::{File, OpenOptions};
use std::path::Path;

pub(super) fn acquire(path: &Path) -> ReplicaResult<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(storage)?;
    }
    let canonical = if path.exists() {
        path.canonicalize().map_err(storage)?
    } else {
        path.parent()
            .unwrap_or(Path::new("."))
            .canonicalize()
            .map_err(storage)?
            .join(
                path.file_name()
                    .ok_or_else(|| ReplicaError::Storage("invalid store path".into()))?,
            )
    };
    let mut name = canonical.into_os_string();
    name.push(".author.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(name)
        .map_err(storage)?;
    file.try_lock().map_err(|error| {
        ReplicaError::Storage(format!("Store already has an authoring engine: {error}"))
    })?;
    Ok(file)
}

fn storage(error: std::io::Error) -> ReplicaError {
    ReplicaError::Storage(error.to_string())
}
