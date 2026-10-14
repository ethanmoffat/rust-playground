use std::{collections::HashMap, error::Error, sync::Arc};

use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
use arrowcol_protocol::fingerprint_schema;

#[test]
fn field_reorder_type_nullability_and_metadata_mutations_change_fingerprint() {
    let base = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let reordered = Schema::new(vec![
        Field::new("b", DataType::Utf8, true),
        Field::new("a", DataType::Int64, false),
    ]);
    let changed_type = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let changed_nullability = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let changed_metadata = Schema::new(vec![
        Field::new("a", DataType::Int64, false)
            .with_metadata(HashMap::from([("semantic".to_owned(), "counter".to_owned())])),
        Field::new("b", DataType::Utf8, true),
    ]);

    let base = fingerprint_schema(&base);
    assert_ne!(base, fingerprint_schema(&reordered));
    assert_ne!(base, fingerprint_schema(&changed_type));
    assert_ne!(base, fingerprint_schema(&changed_nullability));
    assert_ne!(base, fingerprint_schema(&changed_metadata));
}

#[test]
fn metadata_map_insertion_order_does_not_change_fingerprint() {
    let mut left_schema_metadata = HashMap::new();
    left_schema_metadata.insert("z".to_owned(), "last".to_owned());
    left_schema_metadata.insert("a".to_owned(), "first".to_owned());
    let mut left_field_metadata = HashMap::new();
    left_field_metadata.insert("unit".to_owned(), "bytes".to_owned());
    left_field_metadata.insert("source".to_owned(), "sdk".to_owned());

    let mut right_schema_metadata = HashMap::new();
    right_schema_metadata.insert("a".to_owned(), "first".to_owned());
    right_schema_metadata.insert("z".to_owned(), "last".to_owned());
    let mut right_field_metadata = HashMap::new();
    right_field_metadata.insert("source".to_owned(), "sdk".to_owned());
    right_field_metadata.insert("unit".to_owned(), "bytes".to_owned());

    let left = Schema::new_with_metadata(
        vec![Field::new("bytes", DataType::UInt64, false).with_metadata(left_field_metadata)],
        left_schema_metadata,
    );
    let right = Schema::new_with_metadata(
        vec![Field::new("bytes", DataType::UInt64, false).with_metadata(right_field_metadata)],
        right_schema_metadata,
    );
    assert_eq!(fingerprint_schema(&left), fingerprint_schema(&right));
}

#[test]
fn nested_time_decimal_dictionary_list_map_and_union_details_are_canonical() -> Result<(), Box<dyn Error>> {
    let timestamp_ns_utc = Schema::new(vec![Field::new(
        "at",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    )]);
    let timestamp_us_utc = Schema::new(vec![Field::new(
        "at",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )]);
    let timestamp_ns_offset = Schema::new(vec![Field::new(
        "at",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
        false,
    )]);
    assert_ne!(
        fingerprint_schema(&timestamp_ns_utc),
        fingerprint_schema(&timestamp_us_utc)
    );
    assert_ne!(
        fingerprint_schema(&timestamp_ns_utc),
        fingerprint_schema(&timestamp_ns_offset)
    );

    let decimal = Schema::new(vec![Field::new("amount", DataType::Decimal128(20, 4), false)]);
    let changed_precision = Schema::new(vec![Field::new("amount", DataType::Decimal128(21, 4), false)]);
    assert_ne!(fingerprint_schema(&decimal), fingerprint_schema(&changed_precision));

    let list = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        false,
    )]);
    let changed_list_child = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        false,
    )]);
    assert_ne!(fingerprint_schema(&list), fingerprint_schema(&changed_list_child));

    let dictionary_field = Field::new(
        "kind",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        false,
    );
    let dictionary = Schema::new(vec![dictionary_field.clone()]);
    let ordered_dictionary = Schema::new(vec![dictionary_field.with_dict_is_ordered(true)]);
    assert_ne!(fingerprint_schema(&dictionary), fingerprint_schema(&ordered_dictionary));

    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]
            .into(),
        ),
        false,
    ));
    let map = Schema::new(vec![Field::new(
        "attributes",
        DataType::Map(Arc::clone(&entries), false),
        true,
    )]);
    let sorted_map = Schema::new(vec![Field::new("attributes", DataType::Map(entries, true), true)]);
    assert_ne!(fingerprint_schema(&map), fingerprint_schema(&sorted_map));

    let union_fields = UnionFields::try_new(
        vec![1, 7],
        vec![
            Field::new("integer", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ],
    )?;
    let changed_union_ids = UnionFields::try_new(
        vec![2, 7],
        vec![
            Field::new("integer", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ],
    )?;
    let dense = Schema::new(vec![Field::new(
        "value",
        DataType::Union(union_fields.clone(), UnionMode::Dense),
        true,
    )]);
    let sparse = Schema::new(vec![Field::new(
        "value",
        DataType::Union(union_fields, UnionMode::Sparse),
        true,
    )]);
    let changed_ids = Schema::new(vec![Field::new(
        "value",
        DataType::Union(changed_union_ids, UnionMode::Dense),
        true,
    )]);
    assert_ne!(fingerprint_schema(&dense), fingerprint_schema(&sparse));
    assert_ne!(fingerprint_schema(&dense), fingerprint_schema(&changed_ids));
    Ok(())
}

#[test]
fn representative_schema_has_stable_snapshot() -> Result<(), Box<dyn Error>> {
    let union = UnionFields::try_new(
        vec![3, 9],
        vec![
            Field::new("count", DataType::UInt64, false),
            Field::new("message", DataType::Utf8, true),
        ],
    )?;
    let schema = Schema::new_with_metadata(
        vec![
            Field::new(
                "observed_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "samples",
                DataType::List(Arc::new(Field::new("item", DataType::Decimal128(24, 6), true))),
                true,
            ),
            Field::new("payload", DataType::Union(union, UnionMode::Dense), true),
        ],
        HashMap::from([("schema_owner".to_owned(), "telemetry".to_owned())]),
    );
    assert_eq!(
        fingerprint_schema(&schema).to_string(),
        "sha256:03b83feef49531c9fabfe7b5cdf2024ff33b803087fabf5060b13057f2aeb828"
    );
    Ok(())
}
