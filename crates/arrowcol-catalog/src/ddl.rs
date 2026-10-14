use std::collections::BTreeSet;

use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};
use sqlparser::{
    ast::{
        AlterColumnOperation, AlterTable, AlterTableOperation, ArrayElemTypeDef, ColumnDef, ColumnOption, CreateTable,
        CreateTableOptions, DataType, ExactNumberInfo, HiveDistributionStyle, Ident, ObjectName, Statement,
        StructField, TimezoneInfo,
    },
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};
use uuid::Uuid;

use crate::{
    CatalogError, CatalogErrorCode, CatalogField, CatalogResult, CatalogType, FieldPath, ListKind, PartitionMetadata,
    TimeUnit, TypeMapping, deterministic_field_id, deterministic_table_id,
};

const MAX_DDL_BYTES: usize = 65_536;

/// Parsed catalog DDL command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DdlCommand {
    /// Create a new table and its first immutable schema version.
    CreateTable(ParsedCreateTable),
    /// Append nullable fields to an existing table.
    AddColumns(ParsedAddColumns),
}

impl DdlCommand {
    /// Returns the target table.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        match self {
            Self::CreateTable(command) => command.identity(),
            Self::AddColumns(command) => command.identity(),
        }
    }

    /// Returns canonical DDL used as the idempotency input.
    #[must_use]
    pub fn canonical_ddl(&self) -> &str {
        match self {
            Self::CreateTable(command) => command.canonical_ddl(),
            Self::AddColumns(command) => command.canonical_ddl(),
        }
    }
}

/// Parsed `CREATE TABLE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCreateTable {
    identity: TableIdentity,
    fields: Vec<CatalogField>,
    canonical_ddl: String,
}

impl ParsedCreateTable {
    /// Returns the target identity.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        &self.identity
    }

    /// Returns all ordered data and partition fields.
    #[must_use]
    pub fn fields(&self) -> &[CatalogField] {
        &self.fields
    }

    /// Returns canonical normalized DDL.
    #[must_use]
    pub fn canonical_ddl(&self) -> &str {
        &self.canonical_ddl
    }

    /// Consumes the parsed command into its parts.
    #[must_use]
    pub fn into_parts(self) -> (TableIdentity, Vec<CatalogField>, String) {
        (self.identity, self.fields, self.canonical_ddl)
    }
}

/// Parsed `ALTER TABLE ... ADD COLUMNS`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedAddColumns {
    identity: TableIdentity,
    fields: Vec<FieldBlueprint>,
    canonical_ddl: String,
}

impl ParsedAddColumns {
    /// Returns the target identity.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        &self.identity
    }

    /// Returns the nullable fields to append.
    #[must_use]
    pub fn fields(&self) -> &[FieldBlueprint] {
        &self.fields
    }

    /// Returns canonical normalized DDL.
    #[must_use]
    pub fn canonical_ddl(&self) -> &str {
        &self.canonical_ddl
    }
}

/// Field parsed from an append command before its never-reused position and
/// protobuf number are allocated from the current schema head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldBlueprint {
    id: Uuid,
    name: String,
    nullable: bool,
    mapping: TypeMapping,
}

impl FieldBlueprint {
    /// Returns the deterministic field identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the normalized field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether the field is nullable.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the cross-system type mapping.
    #[must_use]
    pub const fn mapping(&self) -> &TypeMapping {
        &self.mapping
    }

    pub(crate) fn materialize(&self, position: u32, field_number: u32) -> CatalogResult<CatalogField> {
        CatalogField::new(
            self.id,
            self.name.clone(),
            position,
            field_number,
            self.nullable,
            None,
            self.mapping.clone(),
        )
    }
}

/// Strict maintained-parser front end for catalog DDL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlParser {
    timestamp_timezone: String,
}

impl Default for DdlParser {
    fn default() -> Self {
        Self {
            timestamp_timezone: "UTC".to_owned(),
        }
    }
}

impl DdlParser {
    /// Creates a parser with the timezone used by `TIMESTAMP WITH TIME ZONE`.
    #[must_use]
    pub fn new(timestamp_timezone: impl Into<String>) -> Self {
        Self {
            timestamp_timezone: timestamp_timezone.into(),
        }
    }

    /// Parses one strict catalog DDL statement.
    ///
    /// Hive's plural `ADD COLUMNS (...)` spelling is normalized token-by-token
    /// into standard `ADD COLUMN` operations and then parsed by
    /// `datafusion-sqlparser-rs`; all semantic validation uses the resulting
    /// maintained AST.
    ///
    /// # Errors
    ///
    /// Returns a structured catalog error for malformed, unsupported,
    /// destructive, partition-changing, reordering, non-null append, or lossy
    /// type requests.
    pub fn parse(&self, sql: &str) -> CatalogResult<DdlCommand> {
        if sql.is_empty() || sql.len() > MAX_DDL_BYTES {
            return Err(CatalogError::new(
                CatalogErrorCode::DdlParse,
                format!("DDL must contain between 1 and {MAX_DDL_BYTES} bytes"),
            ));
        }

        let dialect = GenericDialect {};
        let normalized = normalize_add_columns(sql, &dialect)?;
        let statements = Parser::parse_sql(&dialect, &normalized)
            .map_err(|error| CatalogError::new(CatalogErrorCode::DdlParse, format!("invalid catalog DDL: {error}")))?;
        if statements.len() != 1 {
            return Err(CatalogError::new(
                CatalogErrorCode::DdlUnsupportedStatement,
                "exactly one DDL statement is required",
            ));
        }

        match statements.into_iter().next() {
            Some(Statement::CreateTable(create)) => self.parse_create(create).map(DdlCommand::CreateTable),
            Some(Statement::AlterTable(alter)) => self.parse_alter(alter).map(DdlCommand::AddColumns),
            Some(Statement::Drop { .. } | Statement::Truncate(_)) => Err(CatalogError::new(
                CatalogErrorCode::DdlDestructiveStatement,
                "destructive catalog statements are forbidden",
            )),
            Some(_) => Err(CatalogError::new(
                CatalogErrorCode::DdlUnsupportedStatement,
                "only CREATE TABLE and ALTER TABLE ADD COLUMNS are supported",
            )),
            None => Err(CatalogError::new(CatalogErrorCode::DdlParse, "DDL statement is empty")),
        }
    }

    fn parse_create(&self, create: CreateTable) -> CatalogResult<ParsedCreateTable> {
        validate_create_options(&create)?;
        let identity = parse_identity(&create.name)?;
        let table_id = deterministic_table_id(&identity);
        if create.columns.is_empty() {
            return Err(CatalogError::new(
                CatalogErrorCode::DdlUnsupportedStatement,
                "CREATE TABLE requires at least one field",
            ));
        }

        let mut fields = Vec::new();
        let mut names = BTreeSet::new();
        for column in &create.columns {
            let position = u32::try_from(fields.len()).map_err(|_| too_many_fields())?;
            let field_number = position.checked_add(1).ok_or_else(too_many_fields)?;
            let field = self.materialize_column(
                table_id,
                column,
                position,
                field_number,
                None,
                CatalogErrorCode::DdlUnsupportedStatement,
            )?;
            if !names.insert(field.name().to_owned()) {
                return Err(duplicate_field(field.name()));
            }
            fields.push(field);
        }

        let partition_start = fields.len();
        if let HiveDistributionStyle::PARTITIONED { columns } = &create.hive_distribution {
            for (partition_ordinal, column) in columns.iter().enumerate() {
                let position = u32::try_from(fields.len()).map_err(|_| too_many_fields())?;
                let field_number = position.checked_add(1).ok_or_else(too_many_fields)?;
                let partition_ordinal = u32::try_from(partition_ordinal).map_err(|_| too_many_fields())?;
                let field = self.materialize_column(
                    table_id,
                    column,
                    position,
                    field_number,
                    Some(PartitionMetadata::new(partition_ordinal)),
                    CatalogErrorCode::DdlPartitionMutation,
                )?;
                if !is_hive_partition_type(field.mapping().arrow()) {
                    return Err(CatalogError::new(
                        CatalogErrorCode::TypeMappingUnsupported,
                        format!("partition field {} must use a primitive Hive type", field.name()),
                    ));
                }
                if !names.insert(field.name().to_owned()) {
                    return Err(duplicate_field(field.name()));
                }
                fields.push(field);
            }
        }

        let canonical_ddl = canonical_create(&identity, &fields[..partition_start], &fields[partition_start..]);
        Ok(ParsedCreateTable {
            identity,
            fields,
            canonical_ddl,
        })
    }

    fn parse_alter(&self, alter: AlterTable) -> CatalogResult<ParsedAddColumns> {
        validate_alter_options(&alter)?;
        let identity = parse_identity(&alter.name)?;
        let table_id = deterministic_table_id(&identity);
        if alter.operations.is_empty() {
            return Err(CatalogError::new(
                CatalogErrorCode::DdlUnsupportedStatement,
                "ALTER TABLE ADD COLUMNS requires at least one field",
            ));
        }

        let mut fields = Vec::with_capacity(alter.operations.len());
        let mut names = BTreeSet::new();
        for operation in alter.operations {
            let AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position,
                ..
            } = operation
            else {
                return Err(classify_alter_operation(&operation));
            };
            if if_not_exists {
                return Err(CatalogError::new(
                    CatalogErrorCode::DdlUnsupportedStatement,
                    "ADD COLUMN IF NOT EXISTS is not part of the strict catalog contract",
                ));
            }
            if column_position.is_some() {
                return Err(CatalogError::new(
                    CatalogErrorCode::DdlFieldReorder,
                    "new fields must be appended and cannot use FIRST or AFTER",
                ));
            }
            let (name, nullable, mapping) = self.parse_column(table_id, &column_def)?;
            if !nullable {
                return Err(CatalogError::new(
                    CatalogErrorCode::DdlNonNullableAppend,
                    format!("new field {name} must be nullable"),
                ));
            }
            if !names.insert(name.clone()) {
                return Err(duplicate_field(&name));
            }
            fields.push(FieldBlueprint {
                id: deterministic_field_id(table_id, &FieldPath::root(&name)),
                name,
                nullable,
                mapping,
            });
        }

        let canonical_ddl = canonical_alter(&identity, &fields);
        Ok(ParsedAddColumns {
            identity,
            fields,
            canonical_ddl,
        })
    }

    fn materialize_column(
        &self,
        table_id: Uuid,
        column: &ColumnDef,
        position: u32,
        field_number: u32,
        partition: Option<PartitionMetadata>,
        unsupported_option_code: CatalogErrorCode,
    ) -> CatalogResult<CatalogField> {
        let (name, nullable, mapping) =
            self.parse_column_with_option_code(table_id, column, unsupported_option_code)?;
        let id = deterministic_field_id(table_id, &FieldPath::root(&name));
        CatalogField::new(id, name, position, field_number, nullable, partition, mapping)
    }

    fn parse_column(&self, table_id: Uuid, column: &ColumnDef) -> CatalogResult<(String, bool, TypeMapping)> {
        self.parse_column_with_option_code(table_id, column, CatalogErrorCode::DdlBreakingMutation)
    }

    fn parse_column_with_option_code(
        &self,
        table_id: Uuid,
        column: &ColumnDef,
        unsupported_option_code: CatalogErrorCode,
    ) -> CatalogResult<(String, bool, TypeMapping)> {
        let name = normalize_ident(&column.name);
        validate_field_name(&name)?;
        let nullable = parse_nullability(&column.options, unsupported_option_code)?;
        let path = FieldPath::root(&name);
        let data_type = self.parse_type(table_id, &path, &column.data_type)?;
        Ok((name, nullable, TypeMapping::new(data_type)))
    }

    fn parse_type(&self, table_id: Uuid, path: &FieldPath, data_type: &DataType) -> CatalogResult<CatalogType> {
        let mapped = match data_type {
            DataType::Boolean | DataType::Bool => CatalogType::Boolean,
            DataType::TinyInt(None) => CatalogType::Int8,
            DataType::SmallInt(None) | DataType::Int2(None) | DataType::Int16 => CatalogType::Int16,
            DataType::Int(None) | DataType::Integer(None) | DataType::Int4(None) | DataType::Int32 => {
                CatalogType::Int32
            }
            DataType::BigInt(None) | DataType::Int8(None) | DataType::Int64 => CatalogType::Int64,
            DataType::TinyIntUnsigned(None) | DataType::UInt8 | DataType::UTinyInt => CatalogType::UInt8,
            DataType::SmallIntUnsigned(None)
            | DataType::Int2Unsigned(None)
            | DataType::UInt16
            | DataType::USmallInt => CatalogType::UInt16,
            DataType::IntUnsigned(None)
            | DataType::IntegerUnsigned(None)
            | DataType::Int4Unsigned(None)
            | DataType::UInt32 => CatalogType::UInt32,
            DataType::BigIntUnsigned(None) | DataType::Int8Unsigned(None) | DataType::UInt64 | DataType::UBigInt => {
                CatalogType::UInt64
            }
            DataType::Float(ExactNumberInfo::None) | DataType::Real | DataType::Float4 | DataType::Float32 => {
                CatalogType::Float32
            }
            DataType::Double(ExactNumberInfo::None)
            | DataType::DoublePrecision
            | DataType::Float8
            | DataType::Float64 => CatalogType::Float64,
            DataType::Varchar(None) | DataType::Char(None) | DataType::Text | DataType::String(None) => {
                CatalogType::Utf8
            }
            DataType::Bytea => CatalogType::Binary,
            DataType::Date | DataType::Date32 => CatalogType::Date32,
            DataType::Timestamp(precision, timezone) => CatalogType::Timestamp {
                unit: parse_timestamp_precision(*precision)?,
                timezone: parse_timezone(*timezone, &self.timestamp_timezone)?,
            },
            DataType::Time(precision, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) => {
                let unit = parse_timestamp_precision(*precision)?;
                match unit {
                    TimeUnit::Second | TimeUnit::Millisecond => CatalogType::Time32 { unit },
                    TimeUnit::Microsecond | TimeUnit::Nanosecond => CatalogType::Time64 { unit },
                }
            }
            DataType::Numeric(number) | DataType::Decimal(number) => parse_decimal(*number)?,
            DataType::Array(ArrayElemTypeDef::AngleBracket(element)) => {
                self.parse_list(table_id, path, ListKind::List, element)?
            }
            DataType::Array(ArrayElemTypeDef::SquareBracket(element, size)) => {
                let kind = match size {
                    Some(size) => ListKind::FixedSize(i32::try_from(*size).map_err(|_| {
                        CatalogError::new(
                            CatalogErrorCode::TypeMappingUnsupported,
                            format!("fixed-size list length {size} does not fit i32"),
                        )
                    })?),
                    None => ListKind::List,
                };
                self.parse_list(table_id, path, kind, element)?
            }
            DataType::Struct(fields, _) => self.parse_struct(table_id, path, fields)?,
            DataType::Map(key, value) => self.parse_map(table_id, path, key, value)?,
            DataType::Custom(name, modifiers) => parse_custom_type(name, modifiers)?,
            _ => {
                return Err(CatalogError::new(
                    CatalogErrorCode::TypeMappingUnsupported,
                    format!("SQL type {data_type} has no lossless catalog mapping"),
                ));
            }
        };
        Ok(mapped)
    }

    fn parse_list(
        &self,
        table_id: Uuid,
        path: &FieldPath,
        list_kind: ListKind,
        element_type: &DataType,
    ) -> CatalogResult<CatalogType> {
        if matches!(list_kind, ListKind::FixedSize(size) if size <= 0) {
            return Err(CatalogError::new(
                CatalogErrorCode::TypeMappingUnsupported,
                "fixed-size list length must be positive",
            ));
        }
        let element_path = path.child("item");
        let element = CatalogField::new(
            deterministic_field_id(table_id, &element_path),
            "item",
            0,
            1,
            true,
            None,
            TypeMapping::new(self.parse_type(table_id, &element_path, element_type)?),
        )?;
        Ok(CatalogType::List {
            list_kind,
            element: Box::new(element),
        })
    }

    fn parse_struct(&self, table_id: Uuid, path: &FieldPath, fields: &[StructField]) -> CatalogResult<CatalogType> {
        if fields.is_empty() {
            return Err(CatalogError::new(
                CatalogErrorCode::TypeMappingUnsupported,
                "empty structs are not supported",
            ));
        }
        let mut output = Vec::with_capacity(fields.len());
        let mut names = BTreeSet::new();
        for (index, field) in fields.iter().enumerate() {
            if field.options.as_ref().is_some_and(|options| !options.is_empty()) {
                return Err(CatalogError::new(
                    CatalogErrorCode::DdlUnsupportedStatement,
                    "nested struct field options are not supported",
                ));
            }
            let name = field
                .field_name
                .as_ref()
                .map_or_else(|| format!("c{index}"), normalize_ident);
            validate_field_name(&name)?;
            if !names.insert(name.clone()) {
                return Err(duplicate_field(&name));
            }
            let position = u32::try_from(index).map_err(|_| too_many_fields())?;
            let field_number = position.checked_add(1).ok_or_else(too_many_fields)?;
            let field_path = path.child(&name);
            output.push(CatalogField::new(
                deterministic_field_id(table_id, &field_path),
                name,
                position,
                field_number,
                true,
                None,
                TypeMapping::new(self.parse_type(table_id, &field_path, &field.field_type)?),
            )?);
        }
        Ok(CatalogType::Struct { fields: output })
    }

    fn parse_map(
        &self,
        table_id: Uuid,
        path: &FieldPath,
        key_type: &DataType,
        value_type: &DataType,
    ) -> CatalogResult<CatalogType> {
        let key_path = path.child("key");
        let value_path = path.child("value");
        let key = CatalogField::new(
            deterministic_field_id(table_id, &key_path),
            "key",
            0,
            1,
            false,
            None,
            TypeMapping::new(self.parse_type(table_id, &key_path, key_type)?),
        )?;
        if !is_protobuf_map_key(key.mapping().arrow()) {
            return Err(CatalogError::new(
                CatalogErrorCode::TypeMappingUnsupported,
                format!("map key type {key_type} is not portable across Hive and protobuf"),
            ));
        }
        let value = CatalogField::new(
            deterministic_field_id(table_id, &value_path),
            "value",
            1,
            2,
            true,
            None,
            TypeMapping::new(self.parse_type(table_id, &value_path, value_type)?),
        )?;
        Ok(CatalogType::Map {
            key: Box::new(key),
            value: Box::new(value),
            keys_sorted: false,
        })
    }
}

fn validate_create_options(create: &CreateTable) -> CatalogResult<()> {
    if create.or_replace {
        return Err(CatalogError::new(
            CatalogErrorCode::DdlDestructiveStatement,
            "CREATE OR REPLACE is destructive and forbidden",
        ));
    }
    if create.if_not_exists {
        return Err(CatalogError::new(
            CatalogErrorCode::DdlUnsupportedStatement,
            "IF NOT EXISTS is replaced by catalog idempotency and is not accepted",
        ));
    }
    if create.temporary
        || create.external
        || create.dynamic
        || create.global.is_some()
        || create.transient
        || create.volatile
        || create.iceberg
        || create.snapshot
        || !create.constraints.is_empty()
        || create.hive_formats.is_some()
        || !matches!(create.table_options, CreateTableOptions::None)
        || create.file_format.is_some()
        || create.location.is_some()
        || create.query.is_some()
        || create.without_rowid
        || create.like.is_some()
        || create.clone.is_some()
        || create.version.is_some()
        || create.comment.is_some()
        || create.on_commit.is_some()
        || create.on_cluster.is_some()
        || create.primary_key.is_some()
        || create.order_by.is_some()
        || create.partition_by.is_some()
        || create.cluster_by.is_some()
        || create.clustered_by.is_some()
        || create.inherits.is_some()
        || create.partition_of.is_some()
        || create.for_values.is_some()
        || create.strict
        || create.copy_grants
        || create.enable_schema_evolution.is_some()
        || create.change_tracking.is_some()
        || create.data_retention_time_in_days.is_some()
        || create.max_data_extension_time_in_days.is_some()
        || create.default_ddl_collation.is_some()
        || create.with_aggregation_policy.is_some()
        || create.with_row_access_policy.is_some()
        || create.with_storage_lifecycle_policy.is_some()
        || create.with_tags.is_some()
        || create.external_volume.is_some()
        || create.base_location.is_some()
        || create.catalog.is_some()
        || create.catalog_sync.is_some()
        || create.storage_serialization_policy.is_some()
        || create.target_lag.is_some()
        || create.warehouse.is_some()
        || create.refresh_mode.is_some()
        || create.initialize.is_some()
        || create.require_user
        || create.diststyle.is_some()
        || create.distkey.is_some()
        || create.sortkey.is_some()
        || create.backup.is_some()
        || matches!(create.hive_distribution, HiveDistributionStyle::SKEWED { .. })
    {
        return Err(CatalogError::new(
            CatalogErrorCode::DdlUnsupportedStatement,
            "CREATE TABLE contains an option outside the strict catalog subset",
        ));
    }
    Ok(())
}

fn validate_alter_options(alter: &AlterTable) -> CatalogResult<()> {
    if alter.if_exists
        || alter.only
        || alter.location.is_some()
        || alter.on_cluster.is_some()
        || alter.table_type.is_some()
    {
        return Err(CatalogError::new(
            CatalogErrorCode::DdlUnsupportedStatement,
            "ALTER TABLE options are outside the strict ADD COLUMNS subset",
        ));
    }
    Ok(())
}

fn classify_alter_operation(operation: &AlterTableOperation) -> CatalogError {
    let code = match operation {
        AlterTableOperation::AddPartitions { .. }
        | AlterTableOperation::DropPartitions { .. }
        | AlterTableOperation::RenamePartitions { .. }
        | AlterTableOperation::AttachPartition { .. }
        | AlterTableOperation::DetachPartition { .. }
        | AlterTableOperation::FreezePartition { .. }
        | AlterTableOperation::UnfreezePartition { .. } => CatalogErrorCode::DdlPartitionMutation,
        AlterTableOperation::DropColumn { .. }
        | AlterTableOperation::RenameColumn { .. }
        | AlterTableOperation::RenameTable { .. }
        | AlterTableOperation::ChangeColumn { .. }
        | AlterTableOperation::ModifyColumn { .. }
        | AlterTableOperation::SwapWith { .. }
        | AlterTableOperation::AlterColumn {
            op:
                AlterColumnOperation::SetNotNull
                | AlterColumnOperation::DropNotNull
                | AlterColumnOperation::SetDataType { .. },
            ..
        } => CatalogErrorCode::DdlBreakingMutation,
        _ => CatalogErrorCode::DdlUnsupportedStatement,
    };
    CatalogError::new(
        code,
        format!("ALTER TABLE operation {operation} is not an append-only field addition"),
    )
}

fn parse_identity(name: &ObjectName) -> CatalogResult<TableIdentity> {
    let parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident().map(normalize_ident).ok_or_else(|| {
                CatalogError::new(
                    CatalogErrorCode::InvalidIdentity,
                    "computed object-name parts are not supported",
                )
            })
        })
        .collect::<CatalogResult<Vec<_>>>()?;
    let [catalog, schema, table] = parts.as_slice() else {
        return Err(CatalogError::new(
            CatalogErrorCode::InvalidIdentity,
            "table identity must contain exactly catalog.schema.table",
        ));
    };
    let catalog = CatalogName::new(catalog.clone())
        .map_err(|error| CatalogError::new(CatalogErrorCode::InvalidIdentity, error.to_string()))?;
    let schema = SchemaName::new(schema.clone())
        .map_err(|error| CatalogError::new(CatalogErrorCode::InvalidIdentity, error.to_string()))?;
    let table = TableName::new(table.clone())
        .map_err(|error| CatalogError::new(CatalogErrorCode::InvalidIdentity, error.to_string()))?;
    Ok(TableIdentity::new(catalog, schema, table))
}

fn normalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn validate_field_name(name: &str) -> CatalogResult<()> {
    TableName::new(name.to_owned())
        .map(|_| ())
        .map_err(|error| CatalogError::new(CatalogErrorCode::InvalidIdentity, error.to_string()))
}

fn parse_nullability(
    options: &[sqlparser::ast::ColumnOptionDef],
    unsupported_option_code: CatalogErrorCode,
) -> CatalogResult<bool> {
    let mut nullable = true;
    let mut nullability_seen = false;
    for option in options {
        if option.name.is_some() {
            return Err(CatalogError::new(
                unsupported_option_code,
                "named column constraints are not supported",
            ));
        }
        match option.option {
            ColumnOption::Null => {
                if nullability_seen {
                    return Err(CatalogError::new(
                        unsupported_option_code,
                        "column nullability was specified more than once",
                    ));
                }
                nullable = true;
                nullability_seen = true;
            }
            ColumnOption::NotNull => {
                if nullability_seen {
                    return Err(CatalogError::new(
                        unsupported_option_code,
                        "column nullability was specified more than once",
                    ));
                }
                nullable = false;
                nullability_seen = true;
            }
            _ => {
                return Err(CatalogError::new(
                    unsupported_option_code,
                    format!("column option {} is not supported", option.option),
                ));
            }
        }
    }
    Ok(nullable)
}

fn parse_timestamp_precision(precision: Option<u64>) -> CatalogResult<TimeUnit> {
    match precision {
        None | Some(9) => Ok(TimeUnit::Nanosecond),
        Some(0) => Ok(TimeUnit::Second),
        Some(3) => Ok(TimeUnit::Millisecond),
        Some(6) => Ok(TimeUnit::Microsecond),
        Some(other) => Err(CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            format!("timestamp precision {other} is not representable by Arrow"),
        )),
    }
}

fn parse_timezone(timezone: TimezoneInfo, configured: &str) -> CatalogResult<Option<String>> {
    match timezone {
        TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => Ok(None),
        TimezoneInfo::Tz | TimezoneInfo::WithTimeZone if !configured.is_empty() => Ok(Some(configured.to_owned())),
        TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => Err(CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            "TIMESTAMP WITH TIME ZONE requires a configured timezone",
        )),
    }
}

fn parse_decimal(number: ExactNumberInfo) -> CatalogResult<CatalogType> {
    let ExactNumberInfo::PrecisionAndScale(precision, scale) = number else {
        return Err(CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            "DECIMAL requires explicit precision and scale",
        ));
    };
    let precision = u8::try_from(precision).map_err(|_| {
        CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            "decimal precision exceeds 255",
        )
    })?;
    let scale = i8::try_from(scale).map_err(|_| {
        CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            "decimal scale does not fit Arrow decimal128",
        )
    })?;
    if precision == 0 || precision > 38 || scale.unsigned_abs() > precision {
        return Err(CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            format!("DECIMAL({precision},{scale}) is outside Arrow/Hive decimal128 bounds"),
        ));
    }
    Ok(CatalogType::Decimal128 { precision, scale })
}

fn parse_custom_type(name: &ObjectName, modifiers: &[String]) -> CatalogResult<CatalogType> {
    let parts = name
        .0
        .iter()
        .map(|part| part.as_ident().map(normalize_ident))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            CatalogError::new(
                CatalogErrorCode::TypeMappingUnsupported,
                format!("custom type {name} is not supported"),
            )
        })?;
    if parts.len() != 1 {
        return Err(CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            format!("custom type {name} is not supported"),
        ));
    }
    let type_name = parts[0].to_ascii_uppercase();
    match (type_name.as_str(), modifiers) {
        ("FIXED_SIZE_BINARY", [bytes]) => {
            let bytes = bytes.parse::<i32>().map_err(|_| {
                CatalogError::new(
                    CatalogErrorCode::TypeMappingUnsupported,
                    "FIXED_SIZE_BINARY requires one positive integer width",
                )
            })?;
            if bytes <= 0 {
                return Err(CatalogError::new(
                    CatalogErrorCode::TypeMappingUnsupported,
                    "FIXED_SIZE_BINARY width must be positive",
                ));
            }
            Ok(CatalogType::FixedSizeBinary { bytes })
        }
        ("LARGE_STRING", []) => Ok(CatalogType::LargeUtf8),
        ("STRING_VIEW", []) => Ok(CatalogType::Utf8View),
        ("LARGE_BINARY", []) => Ok(CatalogType::LargeBinary),
        ("BINARY_VIEW", []) => Ok(CatalogType::BinaryView),
        ("DATE64", []) => Ok(CatalogType::Date64),
        ("TIME32_SECOND", []) => Ok(CatalogType::Time32 { unit: TimeUnit::Second }),
        ("TIME32_MILLISECOND", []) => Ok(CatalogType::Time32 {
            unit: TimeUnit::Millisecond,
        }),
        ("TIME64_MICROSECOND", []) => Ok(CatalogType::Time64 {
            unit: TimeUnit::Microsecond,
        }),
        ("TIME64_NANOSECOND", []) => Ok(CatalogType::Time64 {
            unit: TimeUnit::Nanosecond,
        }),
        ("DURATION_SECOND", []) => Ok(CatalogType::Duration { unit: TimeUnit::Second }),
        ("DURATION_MILLISECOND", []) => Ok(CatalogType::Duration {
            unit: TimeUnit::Millisecond,
        }),
        ("DURATION_MICROSECOND", []) => Ok(CatalogType::Duration {
            unit: TimeUnit::Microsecond,
        }),
        ("DURATION_NANOSECOND", []) => Ok(CatalogType::Duration {
            unit: TimeUnit::Nanosecond,
        }),
        _ => Err(CatalogError::new(
            CatalogErrorCode::TypeMappingUnsupported,
            format!("custom SQL type {name} has no lossless catalog mapping"),
        )),
    }
}

fn is_hive_partition_type(data_type: &CatalogType) -> bool {
    matches!(
        data_type,
        CatalogType::Boolean
            | CatalogType::Int8
            | CatalogType::Int16
            | CatalogType::Int32
            | CatalogType::Int64
            | CatalogType::UInt8
            | CatalogType::UInt16
            | CatalogType::UInt32
            | CatalogType::UInt64
            | CatalogType::Float32
            | CatalogType::Float64
            | CatalogType::Timestamp { .. }
            | CatalogType::Date32
            | CatalogType::Date64
            | CatalogType::Binary
            | CatalogType::FixedSizeBinary { .. }
            | CatalogType::LargeBinary
            | CatalogType::BinaryView
            | CatalogType::Utf8
            | CatalogType::LargeUtf8
            | CatalogType::Utf8View
            | CatalogType::Decimal128 { .. }
    )
}

fn is_protobuf_map_key(data_type: &CatalogType) -> bool {
    matches!(
        data_type,
        CatalogType::Boolean
            | CatalogType::Int8
            | CatalogType::Int16
            | CatalogType::Int32
            | CatalogType::Int64
            | CatalogType::UInt8
            | CatalogType::UInt16
            | CatalogType::UInt32
            | CatalogType::UInt64
            | CatalogType::Utf8
    )
}

fn canonical_create(identity: &TableIdentity, fields: &[CatalogField], partitions: &[CatalogField]) -> String {
    let mut ddl = format!(
        "CREATE TABLE {} ({})",
        canonical_identity(identity),
        fields.iter().map(canonical_field).collect::<Vec<_>>().join(", ")
    );
    if !partitions.is_empty() {
        ddl.push_str(" PARTITIONED BY (");
        ddl.push_str(&partitions.iter().map(canonical_field).collect::<Vec<_>>().join(", "));
        ddl.push(')');
    }
    ddl
}

fn canonical_alter(identity: &TableIdentity, fields: &[FieldBlueprint]) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMNS ({})",
        canonical_identity(identity),
        fields
            .iter()
            .map(|field| canonical_field_parts(&field.name, &field.mapping, field.nullable))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn canonical_field(field: &CatalogField) -> String {
    canonical_field_parts(field.name(), field.mapping(), field.is_nullable())
}

fn canonical_field_parts(name: &str, mapping: &TypeMapping, nullable: bool) -> String {
    format!(
        "{} {} {}",
        quote_ident(name),
        mapping.datafusion(),
        if nullable { "NULL" } else { "NOT NULL" }
    )
}

fn canonical_identity(identity: &TableIdentity) -> String {
    [
        quote_ident(identity.catalog().as_str()),
        quote_ident(identity.schema().as_str()),
        quote_ident(identity.table().as_str()),
    ]
    .join(".")
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn normalize_add_columns(sql: &str, dialect: &GenericDialect) -> CatalogResult<String> {
    let tokens = Tokenizer::new(dialect, sql)
        .tokenize()
        .map_err(|error| CatalogError::new(CatalogErrorCode::DdlParse, error.to_string()))?
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();

    let Some(add_index) = tokens
        .windows(2)
        .position(|pair| is_word(&pair[0], "ADD") && is_word(&pair[1], "COLUMNS"))
    else {
        return Ok(sql.to_owned());
    };
    if add_index < 3
        || !is_word(&tokens[0], "ALTER")
        || !is_word(&tokens[1], "TABLE")
        || !matches!(tokens.get(add_index + 2), Some(Token::LParen))
    {
        return Err(CatalogError::new(
            CatalogErrorCode::DdlParse,
            "ADD COLUMNS must use ALTER TABLE name ADD COLUMNS (...)",
        ));
    }

    let mut groups = Vec::<Vec<Token>>::new();
    let mut current = Vec::new();
    let mut paren_depth = 0_i32;
    let mut angle_depth = 0_i32;
    let mut bracket_depth = 0_i32;
    let mut closing_index = None;
    for (index, token) in tokens.iter().enumerate().skip(add_index + 3) {
        match token {
            Token::LParen => paren_depth += 1,
            Token::RParen if paren_depth == 0 && angle_depth == 0 && bracket_depth == 0 => {
                closing_index = Some(index);
                if !current.is_empty() {
                    groups.push(std::mem::take(&mut current));
                }
                break;
            }
            Token::RParen => paren_depth -= 1,
            Token::Lt => angle_depth += 1,
            Token::Gt => angle_depth -= 1,
            Token::ShiftRight => angle_depth -= 2,
            Token::LBracket => bracket_depth += 1,
            Token::RBracket => bracket_depth -= 1,
            Token::Comma if paren_depth == 0 && angle_depth == 0 && bracket_depth == 0 => {
                if current.is_empty() {
                    return Err(CatalogError::new(
                        CatalogErrorCode::DdlParse,
                        "ADD COLUMNS contains an empty field definition",
                    ));
                }
                groups.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        if paren_depth < 0 || angle_depth < 0 || bracket_depth < 0 {
            return Err(CatalogError::new(
                CatalogErrorCode::DdlParse,
                "ADD COLUMNS contains unbalanced delimiters",
            ));
        }
        current.push(token.clone());
    }
    let closing_index = closing_index.ok_or_else(|| {
        CatalogError::new(
            CatalogErrorCode::DdlParse,
            "ADD COLUMNS is missing its closing parenthesis",
        )
    })?;
    if groups.is_empty() {
        return Err(CatalogError::new(
            CatalogErrorCode::DdlParse,
            "ADD COLUMNS requires at least one field",
        ));
    }
    if tokens[closing_index + 1..]
        .iter()
        .any(|token| !matches!(token, Token::SemiColon))
    {
        return Err(CatalogError::new(
            CatalogErrorCode::DdlUnsupportedStatement,
            "tokens after ADD COLUMNS are not supported",
        ));
    }

    let prefix = render_tokens(&tokens[..add_index]);
    let operations = groups
        .iter()
        .map(|group| format!("ADD COLUMN {}", render_tokens(group)))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("{prefix} {operations}"))
}

fn render_tokens(tokens: &[Token]) -> String {
    tokens.iter().map(ToString::to_string).collect::<Vec<_>>().join(" ")
}

fn is_word(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case(expected))
}

fn duplicate_field(name: &str) -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::DdlBreakingMutation,
        format!("duplicate field name {name}"),
    )
}

fn too_many_fields() -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::TypeMappingUnsupported,
        "field count exceeds the supported u32 range",
    )
}
