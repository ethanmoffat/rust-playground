mod common;

use std::fs;

use arrowcol_catalog::{CatalogField, CatalogSchemaVersion, CatalogType, SchemaVersion, TypeMapping};
use arrowcol_sdk_gen::{ArtifactBundle, SafeOutputPath, SdkGenError, generate_sdk};

use common::{fixture_schema, reset, safe, workspace};

#[test]
fn output_paths_cannot_escape_or_target_source_trees() {
    for path in [
        "../outside",
        "/absolute/output",
        "crates/arrowcol-sdk-gen/generated",
        "target",
        ".temp",
        "target/../source",
    ] {
        assert!(SafeOutputPath::new(workspace(), path).is_err(), "{path}");
    }
    assert!(SafeOutputPath::new(workspace(), "target/sdk").is_ok());
    assert!(SafeOutputPath::new(workspace(), "crates/arrowcol-sdk-gen/.temp/sdk").is_ok());
}

#[test]
fn existing_symlink_parent_is_rejected() {
    let root = safe("target/arrowcol-sdk-gen-tests/symlink-root");
    reset(&root);
    fs::create_dir_all(root.absolute()).unwrap_or_else(|error| panic!("create symlink test root: {error}"));
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(workspace().join("crates"), root.absolute().join("escape"))
            .unwrap_or_else(|error| panic!("create symlink: {error}"));
        assert!(SafeOutputPath::new(workspace(), "target/arrowcol-sdk-gen-tests/symlink-root/escape/sdk").is_err());
    }
}

#[test]
fn sanitized_identifier_collisions_fail_closed() {
    let base = fixture_schema();
    let template = &base.fields()[0];
    let first = CatalogField::new(
        template.id(),
        "type",
        0,
        1,
        false,
        None,
        TypeMapping::new(CatalogType::UInt64),
    )
    .unwrap_or_else(|error| panic!("first collision field: {error}"));
    let second = CatalogField::new(
        base.fields()[1].id(),
        "type_field",
        1,
        2,
        false,
        None,
        TypeMapping::new(CatalogType::UInt64),
    )
    .unwrap_or_else(|error| panic!("second collision field: {error}"));
    let schema = CatalogSchemaVersion::new(
        base.table_id(),
        base.identity().clone(),
        SchemaVersion::FIRST,
        vec![first, second],
        "collision",
    )
    .unwrap_or_else(|error| panic!("collision schema: {error}"));
    let bundle = ArtifactBundle::from_schema(schema).unwrap_or_else(|error| panic!("collision artifacts: {error}"));
    let output = safe("target/arrowcol-sdk-gen-tests/collision");
    reset(&output);
    let Err(error) = generate_sdk(&bundle, &output) else {
        panic!("collision must fail");
    };
    assert!(matches!(error, SdkGenError::IdentifierCollision { .. }));
}
