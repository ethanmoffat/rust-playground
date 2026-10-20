//! Display and export of result values.

use kusto_explorer_api::{ResultColumn, ResultTable};
use serde_json::Value;

/// Returns whether a KQL scalar type is numeric.
#[must_use]
pub fn is_numeric(column_type: &str) -> bool {
    matches!(column_type, "int" | "long" | "real" | "decimal")
}

/// Returns a value as plain display text.
#[must_use]
pub fn display(value: &Value, column_type: &str) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) if column_type == "datetime" => text.replacen('T', " ", 1).trim_end_matches('Z').to_owned(),
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

/// Returns a numeric value for charts and sorting.
#[must_use]
pub fn as_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok().filter(|number| number.is_finite()),
        Value::Bool(flag) => Some(f64::from(u8::from(*flag))),
        _ => None,
    }
}

/// Orders two cell values; nulls sort last.
#[must_use]
pub fn compare(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Number(a), Value::Number(b)) => match (a.as_i64(), b.as_i64()) {
            (Some(a), Some(b)) => a.cmp(&b),
            _ => a
                .as_f64()
                .unwrap_or(f64::NAN)
                .total_cmp(&b.as_f64().unwrap_or(f64::NAN)),
        },
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::String(a), Value::String(b)) => match (a.parse::<f64>(), b.parse::<f64>()) {
            (Ok(a), Ok(b)) if a.is_finite() && b.is_finite() => a.total_cmp(&b),
            _ => a.cmp(b),
        },
        _ => left.to_string().cmp(&right.to_string()),
    }
}

/// Indices of the rows that match `filter` (case-insensitive, any cell), ordered by `sort`
/// (`(column, descending)`); ties keep their original order and nulls always sort last.
#[must_use]
pub fn visible_rows(table: &ResultTable, filter: &str, sort: Option<(usize, bool)>) -> Vec<usize> {
    let needle = filter.trim().to_lowercase();
    let mut rows: Vec<usize> = if needle.is_empty() {
        (0..table.rows.len()).collect()
    } else {
        (0..table.rows.len())
            .filter(|index| {
                table.rows[*index].iter().zip(&table.columns).any(|(value, column)| {
                    !value.is_null() && display(value, &column.column_type).to_lowercase().contains(&needle)
                })
            })
            .collect()
    };
    if let Some((column, descending)) = sort.filter(|(column, _)| *column < table.columns.len()) {
        let null = Value::Null;
        rows.sort_by(|left, right| {
            let left = table.rows[*left].get(column).unwrap_or(&null);
            let right = table.rows[*right].get(column).unwrap_or(&null);
            match (left.is_null(), right.is_null()) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ if descending => compare(right, left),
                _ => compare(left, right),
            }
        });
    }
    rows
}

/// Supported text export formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// Comma-separated values.
    Csv,
    /// Tab-separated values for spreadsheets.
    Tsv,
    /// JSON array of row objects.
    Json,
    /// Markdown table.
    Markdown,
    /// KQL `datatable` expression.
    Datatable,
}

impl Format {
    /// File extension.
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Tsv => "tsv",
            Self::Json => "json",
            Self::Markdown => "md",
            Self::Datatable => "kql",
        }
    }

    /// MIME type.
    #[must_use]
    pub fn mime(self) -> &'static str {
        match self {
            Self::Csv => "text/csv",
            Self::Tsv => "text/tab-separated-values",
            Self::Json => "application/json",
            Self::Markdown => "text/markdown",
            Self::Datatable => "text/plain",
        }
    }

    /// Display label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Csv => "CSV",
            Self::Tsv => "TSV",
            Self::Json => "JSON",
            Self::Markdown => "Markdown",
            Self::Datatable => "KQL datatable",
        }
    }
}

/// Renders the selected `rows` of `table` in `format`.
#[must_use]
pub fn export(table: &ResultTable, rows: &[usize], format: Format) -> String {
    let rows = rows.iter().filter_map(|index| table.rows.get(*index));
    match format {
        Format::Csv => delimited(&table.columns, rows, ','),
        Format::Tsv => delimited(&table.columns, rows, '\t'),
        Format::Json => json(&table.columns, rows),
        Format::Markdown => markdown(&table.columns, rows),
        Format::Datatable => datatable(&table.columns, rows),
    }
}

fn delimited<'a>(columns: &[ResultColumn], rows: impl Iterator<Item = &'a Vec<Value>>, separator: char) -> String {
    let field = |text: String| -> String {
        if separator == '\t' {
            text.replace(['\t', '\r', '\n'], " ")
        } else if text.contains([',', '"', '\r', '\n']) || text.starts_with(' ') || text.ends_with(' ') {
            format!("\"{}\"", text.replace('"', "\"\""))
        } else {
            text
        }
    };
    let line_end = if separator == '\t' { "\n" } else { "\r\n" };
    let mut out = columns
        .iter()
        .map(|column| field(column.name.clone()))
        .collect::<Vec<_>>()
        .join(&separator.to_string());
    out.push_str(line_end);
    for row in rows {
        let cells = columns.iter().enumerate().map(|(index, column)| {
            let value = row.get(index).unwrap_or(&Value::Null);
            let text = match value {
                Value::String(text) => text.clone(),
                other => display(other, &column.column_type),
            };
            field(text)
        });
        out.push_str(&cells.collect::<Vec<_>>().join(&separator.to_string()));
        out.push_str(line_end);
    }
    out
}

fn json<'a>(columns: &[ResultColumn], rows: impl Iterator<Item = &'a Vec<Value>>) -> String {
    let mut out = String::from("[");
    let mut first = true;
    for row in rows {
        out.push_str(if first { "\n  {" } else { ",\n  {" });
        first = false;
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push_str(&Value::String(column.name.clone()).to_string());
            out.push_str(": ");
            out.push_str(&row.get(index).unwrap_or(&Value::Null).to_string());
        }
        out.push('}');
    }
    out.push_str(if first { "]\n" } else { "\n]\n" });
    out
}

fn markdown<'a>(columns: &[ResultColumn], rows: impl Iterator<Item = &'a Vec<Value>>) -> String {
    let cell = |text: String| {
        text.replace('\\', "\\\\")
            .replace('|', "\\|")
            .replace(['\r', '\n'], " ")
    };
    let mut out = format!(
        "| {} |\n",
        columns
            .iter()
            .map(|column| cell(column.name.clone()))
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let rule = columns
        .iter()
        .map(|column| if is_numeric(&column.column_type) { "---:" } else { "---" });
    out.push_str(&format!("| {} |\n", rule.collect::<Vec<_>>().join(" | ")));
    for row in rows {
        let cells = columns
            .iter()
            .enumerate()
            .map(|(index, column)| cell(display(row.get(index).unwrap_or(&Value::Null), &column.column_type)));
        out.push_str(&format!("| {} |\n", cells.collect::<Vec<_>>().join(" | ")));
    }
    out
}

/// Quotes `text` as a KQL string literal.
#[must_use]
pub fn kql_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for character in text.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Quotes a column or table name when it is not a plain identifier.
#[must_use]
pub fn kql_name(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');
    if plain {
        name.to_owned()
    } else {
        format!("[{}]", kql_string(name))
    }
}

/// Renders a cell value as a KQL literal of its column type.
#[must_use]
pub fn kql_literal(value: &Value, column_type: &str) -> String {
    match (column_type, value) {
        ("string", Value::Null) => "\"\"".to_owned(),
        (_, Value::Null) => format!("{column_type}(null)"),
        ("string", Value::String(text)) => kql_string(text),
        ("bool", Value::Bool(flag)) => flag.to_string(),
        ("int" | "long", Value::Number(number)) => number.to_string(),
        ("real", Value::Number(number)) => format!("real({number})"),
        ("real", Value::String(text)) => match text.as_str() {
            "NaN" => "real(nan)".to_owned(),
            "Infinity" => "real(+inf)".to_owned(),
            "-Infinity" => "real(-inf)".to_owned(),
            other => format!("real({other})"),
        },
        ("decimal" | "datetime" | "timespan" | "guid", Value::String(text)) => format!("{column_type}({text})"),
        ("decimal", Value::Number(number)) => format!("decimal({number})"),
        ("dynamic", other) => format!("dynamic({other})"),
        (_, Value::String(text)) => kql_string(text),
        (_, other) => format!("dynamic({other})"),
    }
}

fn datatable<'a>(columns: &[ResultColumn], rows: impl Iterator<Item = &'a Vec<Value>>) -> String {
    let schema = columns
        .iter()
        .map(|column| format!("{}: {}", kql_name(&column.name), column.column_type));
    let mut out = format!("datatable({})\n[\n", schema.collect::<Vec<_>>().join(", "));
    for row in rows {
        let values = columns
            .iter()
            .enumerate()
            .map(|(index, column)| kql_literal(row.get(index).unwrap_or(&Value::Null), &column.column_type));
        out.push_str(&format!("    {},\n", values.collect::<Vec<_>>().join(", ")));
    }
    out.push(']');
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table() -> ResultTable {
        let column = |name: &str, column_type: &str| ResultColumn {
            name: name.to_owned(),
            column_type: column_type.to_owned(),
        };
        ResultTable {
            name: "PrimaryResult".to_owned(),
            columns: vec![
                column("Name", "string"),
                column("Rows", "long"),
                column("At", "datetime"),
                column("Bag", "dynamic"),
                column("Ratio", "real"),
            ],
            rows: vec![
                vec![
                    json!("a,\"b\""),
                    json!(9_007_199_254_740_993_i64),
                    json!("2026-01-02T03:04:05Z"),
                    json!({"k": [1]}),
                    json!(0.5),
                ],
                vec![
                    json!("pipe|line\nnext"),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    json!("NaN"),
                ],
            ],
            total_rows: 2,
            truncated: false,
        }
    }

    #[test]
    fn csv_quotes_and_preserves_exact_numbers() {
        let csv = export(&table(), &[0, 1], Format::Csv);
        assert_eq!(
            csv,
            "Name,Rows,At,Bag,Ratio\r\n\"a,\"\"b\"\"\",9007199254740993,2026-01-02T03:04:05Z,\"{\"\"k\"\":[1]}\",0.5\r\n\"pipe|line\nnext\",,,,NaN\r\n"
        );
        assert_eq!(
            export(&table(), &[1], Format::Tsv),
            "Name\tRows\tAt\tBag\tRatio\npipe|line next\t\t\t\tNaN\n"
        );
    }

    #[test]
    fn json_keeps_column_order_and_values() {
        let text = export(&table(), &[0], Format::Json);
        assert_eq!(
            text,
            "[\n  {\"Name\": \"a,\\\"b\\\"\", \"Rows\": 9007199254740993, \"At\": \"2026-01-02T03:04:05Z\", \"Bag\": {\"k\":[1]}, \"Ratio\": 0.5}\n]\n"
        );
        let parsed: Value = serde_json::from_str(&text).unwrap_or_default();
        assert_eq!(parsed[0]["Rows"].as_i64(), Some(9_007_199_254_740_993));
        assert_eq!(export(&table(), &[], Format::Json), "[]\n");
    }

    #[test]
    fn markdown_escapes_pipes() {
        let text = export(&table(), &[1], Format::Markdown);
        assert_eq!(
            text,
            "| Name | Rows | At | Bag | Ratio |\n| --- | ---: | --- | --- | ---: |\n| pipe\\|line next |  |  |  | NaN |\n"
        );
    }

    #[test]
    fn datatable_uses_typed_literals() {
        let text = export(&table(), &[0, 1], Format::Datatable);
        assert_eq!(
            text,
            "datatable(Name: string, Rows: long, At: datetime, Bag: dynamic, Ratio: real)\n[\n    \"a,\\\"b\\\"\", 9007199254740993, datetime(2026-01-02T03:04:05Z), dynamic({\"k\":[1]}), real(0.5),\n    \"pipe|line\\nnext\", long(null), datetime(null), dynamic(null), real(nan),\n]\n"
        );
        assert_eq!(kql_name("Table Name"), "[\"Table Name\"]");
        assert_eq!(kql_name("_ok1"), "_ok1");
    }

    #[test]
    fn values_sort_and_display_by_type() {
        use std::cmp::Ordering;
        assert_eq!(compare(&json!(2), &json!(10)), Ordering::Less);
        assert_eq!(
            compare(&json!(9_007_199_254_740_993_i64), &json!(9_007_199_254_740_992_i64)),
            Ordering::Greater
        );
        assert_eq!(compare(&Value::Null, &json!(1)), Ordering::Greater);
        assert_eq!(compare(&json!("b"), &json!("a")), Ordering::Greater);
        assert_eq!(compare(&json!("1.5"), &json!("10")), Ordering::Less);
        assert_eq!(
            display(&json!("2026-01-02T03:04:05.5Z"), "datetime"),
            "2026-01-02 03:04:05.5"
        );
        assert_eq!(as_number(&json!("12.5")), Some(12.5));
        assert_eq!(as_number(&json!("NaN")), None);
        assert!(is_numeric("decimal") && !is_numeric("string"));
    }

    #[test]
    fn visible_rows_filter_and_sort_with_nulls_last() {
        let table = ResultTable {
            name: "T".to_owned(),
            columns: vec![
                ResultColumn {
                    name: "Name".to_owned(),
                    column_type: "string".to_owned(),
                },
                ResultColumn {
                    name: "N".to_owned(),
                    column_type: "long".to_owned(),
                },
            ],
            rows: vec![
                vec![Value::from("alpha"), Value::from(3)],
                vec![Value::from("Beta"), Value::Null],
                vec![Value::from("gamma"), Value::from(10)],
                vec![Value::from("delta"), Value::from(-1)],
            ],
            total_rows: 4,
            truncated: false,
        };
        assert_eq!(visible_rows(&table, "", None), vec![0, 1, 2, 3]);
        assert_eq!(visible_rows(&table, " ET", None), vec![1]);
        assert_eq!(visible_rows(&table, "", Some((1, false))), vec![3, 0, 2, 1]);
        assert_eq!(visible_rows(&table, "", Some((1, true))), vec![2, 0, 3, 1]);
        assert_eq!(visible_rows(&table, "a", Some((0, true))), vec![2, 3, 0, 1]);
        assert_eq!(visible_rows(&table, "", Some((9, true))), vec![0, 1, 2, 3]);
    }
}
