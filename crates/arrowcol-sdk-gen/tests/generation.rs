mod common;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use arrowcol_catalog::{
    CatalogField, CatalogSchemaVersion, CatalogType, FieldPath, ListKind, SchemaVersion, TypeMapping,
    deterministic_field_id,
};
use arrowcol_sdk_gen::{
    ArtifactBundle, SdkGenError, check_generated_sdk, generate_sdk, package_generated_sdk, write_artifact_bundle,
};

use common::{bundle, fixture_schema, reset, safe};

#[test]
fn regeneration_is_byte_deterministic_and_cleans_stale_files() {
    let bundle = bundle();
    let first = safe("target/arrowcol-sdk-gen-tests/deterministic-first");
    let second = safe("target/arrowcol-sdk-gen-tests/deterministic-second");
    reset(&first);
    reset(&second);

    generate_sdk(&bundle, &first).unwrap_or_else(|error| panic!("first generation failed: {error}"));
    generate_sdk(&bundle, &second).unwrap_or_else(|error| panic!("second generation failed: {error}"));
    assert_eq!(read_tree(first.absolute()), read_tree(second.absolute()));
    check_generated_sdk(&bundle, &first).unwrap_or_else(|error| panic!("deterministic check failed: {error}"));

    let stale = first.absolute().join("rust/stale.txt");
    fs::write(&stale, b"stale").unwrap_or_else(|error| panic!("failed to write stale file: {error}"));
    generate_sdk(&bundle, &first).unwrap_or_else(|error| panic!("regeneration failed: {error}"));
    assert!(!stale.exists());
    assert_eq!(read_tree(first.absolute()), read_tree(second.absolute()));
}

#[test]
fn deterministic_check_detects_modified_bytes() {
    let bundle = bundle();
    let output = safe("target/arrowcol-sdk-gen-tests/check-mismatch");
    reset(&output);
    generate_sdk(&bundle, &output).unwrap_or_else(|error| panic!("generation failed: {error}"));
    let source = output.absolute().join("typescript/src/index.ts");
    fs::write(&source, b"modified\n").unwrap_or_else(|error| panic!("failed to mutate output: {error}"));
    let Err(error) = check_generated_sdk(&bundle, &output) else {
        panic!("modified generated output must fail check");
    };
    assert!(matches!(error, SdkGenError::OutputMismatch { .. }));
}

#[test]
fn package_archive_is_byte_deterministic() {
    let bundle = bundle();
    let output = safe("target/arrowcol-sdk-gen-tests/package-input");
    let first = safe("target/arrowcol-sdk-gen-tests/sdk-first.tar.gz");
    let second = safe("target/arrowcol-sdk-gen-tests/sdk-second.tar.gz");
    reset(&output);
    let _ = fs::remove_file(first.absolute());
    let _ = fs::remove_file(second.absolute());
    generate_sdk(&bundle, &output).unwrap_or_else(|error| panic!("generation failed: {error}"));
    let first_result =
        package_generated_sdk(&output, &first).unwrap_or_else(|error| panic!("first package failed: {error}"));
    let second_result =
        package_generated_sdk(&output, &second).unwrap_or_else(|error| panic!("second package failed: {error}"));
    assert_eq!(first_result.sha256, second_result.sha256);
    assert_eq!(
        fs::read(first.absolute()).unwrap_or_else(|error| panic!("read first archive: {error}")),
        fs::read(second.absolute()).unwrap_or_else(|error| panic!("read second archive: {error}"))
    );
}

#[test]
fn artifact_loading_rejects_noncanonical_bytes() {
    let bundle = bundle();
    let output = safe("target/arrowcol-sdk-gen-tests/tampered-artifacts");
    reset(&output);
    write_artifact_bundle(&bundle, &output).unwrap_or_else(|error| panic!("artifact write failed: {error}"));
    let manifest = output.absolute().join("manifest.json");
    let mut bytes = fs::read(&manifest).unwrap_or_else(|error| panic!("read manifest: {error}"));
    bytes.push(b'\n');
    fs::write(&manifest, bytes).unwrap_or_else(|error| panic!("write manifest: {error}"));
    assert!(ArtifactBundle::load_directory(output.absolute()).is_err());
}

#[test]
fn unsupported_list_view_fails_without_replacing_existing_output() {
    let base = fixture_schema();
    let element = CatalogField::new(
        deterministic_field_id(base.table_id(), &FieldPath::root("view").child("item")),
        "item",
        0,
        1,
        true,
        None,
        TypeMapping::new(CatalogType::Utf8),
    )
    .unwrap_or_else(|error| panic!("element must construct: {error}"));
    let field = CatalogField::new(
        deterministic_field_id(base.table_id(), &FieldPath::root("view")),
        "view",
        0,
        1,
        true,
        None,
        TypeMapping::new(CatalogType::List {
            list_kind: ListKind::ListView,
            element: Box::new(element),
        }),
    )
    .unwrap_or_else(|error| panic!("field must construct: {error}"));
    let schema = CatalogSchemaVersion::new(
        base.table_id(),
        base.identity().clone(),
        SchemaVersion::FIRST,
        vec![field],
        "CREATE TABLE fixture",
    )
    .unwrap_or_else(|error| panic!("schema must construct: {error}"));
    let bundle =
        ArtifactBundle::from_schema(schema).unwrap_or_else(|error| panic!("artifacts must construct: {error}"));
    let output = safe("target/arrowcol-sdk-gen-tests/unsupported-atomic");
    reset(&output);
    fs::create_dir_all(output.absolute()).unwrap_or_else(|error| panic!("create sentinel output: {error}"));
    fs::write(output.absolute().join("sentinel"), b"keep").unwrap_or_else(|error| panic!("write sentinel: {error}"));

    let Err(error) = generate_sdk(&bundle, &output) else {
        panic!("ListView generation must fail");
    };
    assert!(matches!(error, SdkGenError::UnsupportedType { .. }));
    assert_eq!(
        fs::read(output.absolute().join("sentinel"))
            .unwrap_or_else(|read_error| panic!("sentinel disappeared: {read_error}")),
        b"keep"
    );
}

#[test]
fn unsupported_dictionary_fails_closed() {
    let base = fixture_schema();
    let field = CatalogField::new(
        deterministic_field_id(base.table_id(), &FieldPath::root("dictionary")),
        "dictionary",
        0,
        1,
        true,
        None,
        TypeMapping::new(CatalogType::Dictionary {
            key: Box::new(CatalogType::Int32),
            value: Box::new(CatalogType::Utf8),
            ordered: false,
        }),
    )
    .unwrap_or_else(|error| panic!("dictionary field must construct: {error}"));
    let schema = CatalogSchemaVersion::new(
        base.table_id(),
        base.identity().clone(),
        SchemaVersion::FIRST,
        vec![field],
        "CREATE TABLE fixture",
    )
    .unwrap_or_else(|error| panic!("dictionary schema must construct: {error}"));
    let bundle = ArtifactBundle::from_schema(schema)
        .unwrap_or_else(|error| panic!("dictionary artifacts must construct: {error}"));
    let output = safe("target/arrowcol-sdk-gen-tests/unsupported-dictionary");
    reset(&output);
    let Err(error) = generate_sdk(&bundle, &output) else {
        panic!("dictionary generation must fail");
    };
    assert!(matches!(error, SdkGenError::UnsupportedType { .. }));
    assert!(!output.absolute().exists());
}

fn read_tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut output = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory).unwrap_or_else(|error| panic!("read {}: {error}", directory.display()));
        for entry in entries {
            let entry = entry.unwrap_or_else(|error| panic!("read directory entry: {error}"));
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                output.insert(
                    path.strip_prefix(root)
                        .unwrap_or_else(|error| panic!("strip prefix: {error}"))
                        .to_path_buf(),
                    fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
                );
            }
        }
    }
    output
}
