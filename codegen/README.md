# Shared generator

For application integration, start with [Models and code generation](../docs/MODELS.md).
This page describes the generator's command-line and file-management behavior.

`bin/replica-codegen` validates a manifest once and renders Swift, Kotlin, or Rust. Ruby 3.4 or newer is required; the development toolchain is pinned in the repository root.

```sh
codegen/bin/replica-codegen --language swift --manifest manifest.json --out Generated --name AppReplica
codegen/bin/replica-codegen --language kotlin --manifest manifest.json --out generated --package com.example.models
codegen/bin/replica-codegen --language rust --manifest manifest.json --out src/generated
```

Use `--document-out` for typed document shapes and `--ts-out` for TypeScript declarations. TypeScript output is a declaration generator, not a JavaScript replication client. `--endpoint` can fetch an HTTP(S) manifest instead of `--manifest`. Use a committed manifest for reproducible builds.

`--check` renders into a temporary directory and fails if checked-in output differs. Rendering and semantic validation finish before destination files are changed. A managed-file ledger records generator-owned files; stale generated files are removed without deleting neighboring handwritten source. Installation is atomic per file, not a multi-directory filesystem transaction; rerunning generation repairs an interrupted installation.

An optional JSON `--config` supports a container `name`, `models` and `variants` name overrides, a Rust `runtimeCrate` alias, and language-specific `documentProjection` adapters. These keep application naming and presentation adapters outside the emitters. Without a projection adapter, document output includes a standalone projection type.

`variants` maps a stream name to `{ "<wire type>": "<emitted name>" }`. It renames the stream's single-table-inheritance variants and the variants of its discriminated-union columns. Without an override, a union variant is named after the last `::` segment of its wire type:

```json
{ "variants": { "jobs": { "ResizeJobResult": "Resize" } } }
```

The semantic model validates stream and type names, lanes, codecs, column directions, nullability, nested shapes, enums, and naming collisions. Native compilers validate the generated public surface in the test matrix. Sharing a semantic model keeps behavior aligned while retaining native emitters; a universal syntax tree would not replace language-specific type-system checks.
