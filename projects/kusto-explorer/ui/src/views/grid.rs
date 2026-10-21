//! Result grid with sorting, column resizing, row virtualization, and row details.

use std::sync::Arc;

use kusto_explorer_api::{ResultColumn, ResultTable};
use leptos::{html, prelude::*};
use send_wrapper::SendWrapper;
use serde_json::Value;
use wasm_bindgen::JsCast;

use super::{chrome::rows_label, icon, open_menu};
use crate::{
    browser::{self, SizeObserver},
    format::{self, Format, kql_literal, kql_name, kql_string},
    state::{App, MenuEntry, Outcome, Tab},
    time,
};

const ROW_HEIGHT: f64 = 24.0;
const VIRTUALIZE_ABOVE: usize = 500;
const OVERSCAN: usize = 10;
const MAX_CELL_CHARS: usize = 500;
const MIN_COLUMN_WIDTH: f64 = 40.0;
const MAX_COLUMN_WIDTH: f64 = 1600.0;

/// A result table shared by the grid's closures.
#[derive(Clone)]
struct TableRef {
    outcome: Arc<Outcome>,
    index: usize,
}

impl TableRef {
    fn get(&self) -> &ResultTable {
        &self.outcome.response.tables[self.index]
    }
}

fn truncate(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        Some((at, _)) => format!("{}…", &text[..at]),
        None => text.to_owned(),
    }
}

fn default_widths(table: &ResultTable) -> Vec<f64> {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let header = column.name.chars().count() + 2;
            let sample = table
                .rows
                .iter()
                .take(200)
                .map(|row| {
                    row.get(index).map_or(0, |value| {
                        format::display(value, &column.column_type).chars().count().min(60)
                    })
                })
                .max()
                .unwrap_or(0);
            (header.max(sample) as f64 * 7.4 + 22.0).clamp(56.0, 420.0)
        })
        .collect()
}

fn gutter_width(rows: usize) -> f64 {
    let digits = rows.max(1).to_string().len() as f64;
    (digits * 8.0 + 18.0).max(40.0)
}

/// Visible slice `[start, end)` of `count` rows for a scroll position.
fn window_for(count: usize, scroll_top: f64, viewport: f64) -> (usize, usize) {
    if count <= VIRTUALIZE_ABOVE {
        return (0, count);
    }
    let first = (scroll_top.max(0.0) / ROW_HEIGHT).floor() as usize;
    let visible = (viewport.max(ROW_HEIGHT) / ROW_HEIGHT).ceil() as usize;
    (
        first.saturating_sub(OVERSCAN).min(count),
        first.saturating_add(visible + OVERSCAN).min(count),
    )
}

/// A `where` clause that keeps (`==`) or drops (`!=`) rows with `value` in `column`.
fn where_clause(column: &ResultColumn, value: &Value, keep: bool) -> String {
    let name = kql_name(&column.name);
    let op = if keep { "==" } else { "!=" };
    let condition = match (value, column.column_type.as_str()) {
        (Value::Null, "string") => format!("{}({name})", if keep { "isempty" } else { "isnotempty" }),
        (Value::Null, _) => format!("{}({name})", if keep { "isnull" } else { "isnotnull" }),
        (_, "dynamic") => format!("tostring({name}) {op} {}", kql_string(&value.to_string())),
        _ => format!("{name} {op} {}", kql_literal(value, &column.column_type)),
    };
    format!("\n| where {condition}")
}

fn row_json(table: &ResultTable, row: usize) -> String {
    let object: serde_json::Map<String, Value> = table
        .columns
        .iter()
        .zip(table.rows.get(row).into_iter().flatten())
        .map(|(column, value)| (column.name.clone(), value.clone()))
        .collect();
    serde_json::to_string_pretty(&Value::Object(object)).unwrap_or_default()
}

fn cell_at(event: &web_sys::MouseEvent) -> Option<(usize, usize, web_sys::Element)> {
    let target = event.target()?.dyn_into::<web_sys::Element>().ok()?;
    let cell = target.closest("[data-col]").ok()??;
    let row = cell.closest("[data-row]").ok()??;
    Some((
        row.get_attribute("data-row")?.parse().ok()?,
        cell.get_attribute("data-col")?.parse().ok()?,
        cell,
    ))
}

fn select_cell(scroll: NodeRef<html::Div>, cell: &web_sys::Element) {
    if let Some(container) = scroll.get_untracked()
        && let Ok(Some(previous)) = container.query_selector(".kx-grid-cell.selected")
    {
        let _ = previous.class_list().remove_1("selected");
    }
    let _ = cell.class_list().add_1("selected");
}

fn cell_menu(app: App, tab: &Tab, table: &TableRef, row: usize, column: usize) -> Vec<MenuEntry> {
    let data = table.get();
    let info = data.columns[column].clone();
    let value = data
        .rows
        .get(row)
        .and_then(|cells| cells.get(column))
        .cloned()
        .unwrap_or(Value::Null);
    let text = format::display(&value, &info.column_type);
    let json = row_json(data, row);
    let (keep, drop) = (where_clause(&info, &value, true), where_clause(&info, &value, false));
    let (filter, details) = (tab.filter.clone(), tab.details.clone());
    let (search, name) = (text.clone(), info.name);
    vec![
        MenuEntry::item("Copy value", "copy", move || app.copy(text.clone(), "the value")),
        MenuEntry::item("Copy row as JSON", "json", move || {
            app.copy(json.clone(), "the row as JSON")
        }),
        MenuEntry::item("Copy column name", "symbol-field", move || {
            app.copy(name.clone(), "the column name")
        }),
        MenuEntry::Separator,
        MenuEntry::item("Insert filter for this value", "filter", move || app.insert(&keep)),
        MenuEntry::item("Insert filter excluding this value", "filter-filled", move || {
            app.insert(&drop)
        }),
        MenuEntry::item("Search results for this value", "search", move || {
            filter.set(search.clone())
        }),
        MenuEntry::Separator,
        MenuEntry::item("Show row details", "eye", move || details.set(Some(row))),
        MenuEntry::item("Copy results as CSV", "copy", move || app.copy_results(Format::Csv)),
    ]
}

fn header_cell(tab: &Tab, index: usize, column: &ResultColumn, widths: Memo<Vec<f64>>) -> AnyView {
    let numeric = format::is_numeric(&column.column_type);
    let state = {
        let sort = tab.sort.clone();
        move || {
            sort.with(|sort| match *sort {
                Some((sorted, true)) if sorted == index => "descending",
                Some((sorted, false)) if sorted == index => "ascending",
                _ => "none",
            })
        }
    };
    let cycle = {
        let sort = tab.sort.clone();
        move |_| {
            sort.update(|sort| {
                *sort = match *sort {
                    Some((sorted, false)) if sorted == index => Some((index, true)),
                    Some((sorted, true)) if sorted == index => None,
                    _ => Some((index, false)),
                };
            });
        }
    };
    let drag = StoredValue::new(None::<(f64, f64)>);
    let (resize, reset) = (tab.widths.clone(), tab.widths.clone());
    let arrow = {
        let state = state.clone();
        move || match state() {
            "ascending" => Some(icon("arrow-up")),
            "descending" => Some(icon("arrow-down")),
            _ => None,
        }
    };
    view! {
        <div
            class="kx-grid-header-cell"
            class:num=numeric
            role="columnheader"
            aria-sort=state
            data-testid="grid-header"
            data-name=column.name.clone()
            title=format!("{}: {} (click to sort)", column.name, column.column_type)
            on:click=cycle
        >
            <span class="kx-grid-header-name">{column.name.clone()}</span>
            {arrow}
            <div
                class="kx-col-resizer"
                title="Drag to resize; double-click to fit"
                on:click=|event| event.stop_propagation()
                on:pointerdown=move |event| {
                    if event.button() != 0 {
                        return;
                    }
                    event.prevent_default();
                    event.stop_propagation();
                    if let Some(target) = event.current_target().and_then(|target| target.dyn_into::<web_sys::Element>().ok()) {
                        let _ = target.set_pointer_capture(event.pointer_id());
                    }
                    let width = widths.with_untracked(|widths| widths.get(index).copied().unwrap_or(120.0));
                    drag.set_value(Some((f64::from(event.client_x()), width)));
                }
                on:pointermove=move |event| {
                    let Some((start, initial)) = drag.get_value() else { return };
                    let width = (initial + f64::from(event.client_x()) - start).clamp(MIN_COLUMN_WIDTH, MAX_COLUMN_WIDTH);
                    resize.update(|widths| {
                        widths.insert(index, width.round());
                    });
                }
                on:pointerup=move |_| drag.set_value(None)
                on:lostpointercapture=move |_| drag.set_value(None)
                on:dblclick=move |event| {
                    event.stop_propagation();
                    reset.update(|widths| {
                        widths.remove(&index);
                    });
                }
            ></div>
        </div>
    }
    .into_any()
}

fn grid_row(table: &TableRef, numeric: &[bool], row: usize, selected: Option<(usize, usize)>) -> AnyView {
    let data = table.get();
    let cells = data
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = data.rows[row].get(index).unwrap_or(&Value::Null);
            let text = format::display(value, &column.column_type);
            let title = (text.chars().count() > 40).then(|| truncate(&text, 2_000));
            view! {
                <div
                    class="kx-grid-cell"
                    class:num=numeric[index]
                    class:null=value.is_null()
                    class:selected=selected == Some((row, index))
                    role="gridcell"
                    data-col=index.to_string()
                    title=title
                >
                    {truncate(&text, MAX_CELL_CHARS)}
                </div>
            }
        })
        .collect_view();
    view! {
        <div class="kx-grid-row" role="row" data-testid="grid-row" data-row=row.to_string()>
            <div class="kx-grid-gutter">{(row + 1).to_string()}</div>
            {cells}
        </div>
    }
    .into_any()
}

fn details_view(app: App, tab: &Tab, table: &TableRef, row: usize) -> AnyView {
    let data = table.get();
    let fields = data
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = data.rows[row].get(index).unwrap_or(&Value::Null);
            let text = match value {
                Value::Array(_) | Value::Object(_) => serde_json::to_string_pretty(value).unwrap_or_default(),
                _ => format::display(value, &column.column_type),
            };
            view! {
                <dt title=column.column_type.clone()>{column.name.clone()} <span class="kx-muted">{column.column_type.clone()}</span></dt>
                <dd class:null=value.is_null()>
                    <pre>{if value.is_null() { "null".to_owned() } else { text }}</pre>
                </dd>
            }
        })
        .collect_view();
    let json = row_json(data, row);
    let close = tab.details.clone();
    view! {
        <aside class="kx-row-details" data-testid="row-details" aria-label="Row details">
            <div class="kx-row-details-header">
                <span>{format!("Row {}", row + 1)}</span>
                <span class="kx-spacer"></span>
                <button class="kx-icon-button" title="Copy row as JSON" aria-label="Copy row as JSON" on:click=move |_| app.copy(json.clone(), "the row as JSON")>
                    {icon("json")}
                </button>
                <button class="kx-icon-button" title="Close" aria-label="Close row details" on:click=move |_| close.set(None)>
                    {icon("close")}
                </button>
            </div>
            <dl class="kx-row-details-body">{fields}</dl>
        </aside>
    }
    .into_any()
}

#[component]
pub(crate) fn Grid(tab: Tab, outcome: Arc<Outcome>, index: usize) -> impl IntoView {
    let app = expect_context::<App>();
    let table = TableRef { outcome, index };
    let defaults = Arc::new(default_widths(table.get()));
    let numeric: Arc<Vec<bool>> = Arc::new(
        table
            .get()
            .columns
            .iter()
            .map(|column| format::is_numeric(&column.column_type))
            .collect(),
    );
    let loaded = table.get().rows.len();
    let total = table.get().total_rows;
    let truncated = table.get().truncated;
    let rows = {
        let (table, filter, sort) = (table.clone(), tab.filter.clone(), tab.sort.clone());
        Memo::new(move |_| filter.with(|filter| format::visible_rows(table.get(), filter, sort.get())))
    };
    let widths = {
        let custom = tab.widths.clone();
        Memo::new(move |_| {
            custom.with(|custom| {
                defaults
                    .iter()
                    .enumerate()
                    .map(|(index, width)| custom.get(&index).copied().unwrap_or(*width))
                    .collect::<Vec<f64>>()
            })
        })
    };
    let gutter = gutter_width(loaded);
    let template = move || {
        widths.with(|widths| {
            let columns: Vec<String> = widths.iter().map(|width| format!("{width}px")).collect();
            let total: f64 = gutter + widths.iter().sum::<f64>();
            format!(
                "--kx-cols: {gutter}px {}; --kx-grid-width: {total}px",
                columns.join(" ")
            )
        })
    };
    let scroll = NodeRef::<html::Div>::new();
    let scroll_top = RwSignal::new(0.0_f64);
    let viewport = RwSignal::new(600.0_f64);
    let observer = StoredValue::new(None::<SendWrapper<SizeObserver>>);
    scroll.on_load(move |element| {
        let observed = browser::observe_size(&element, move |_, height| viewport.set(height));
        observer.set_value(observed.map(SendWrapper::new));
    });
    let window = Memo::new(move |_| window_for(rows.with(Vec::len), scroll_top.get(), viewport.get()));
    let selected = StoredValue::new(None::<(usize, usize)>);
    let headers = table
        .get()
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| header_cell(&tab, index, column, widths))
        .collect_view();
    let on_click = move |event: web_sys::MouseEvent| {
        if let Some((row, column, cell)) = cell_at(&event) {
            selected.set_value(Some((row, column)));
            select_cell(scroll, &cell);
        }
    };
    let on_double_click = {
        let details = tab.details.clone();
        move |event: web_sys::MouseEvent| {
            if let Some((row, _, _)) = cell_at(&event) {
                details.set(Some(row));
            }
        }
    };
    let on_menu = {
        let (tab, table) = (tab.clone(), table.clone());
        move |event: web_sys::MouseEvent| {
            if let Some((row, column, cell)) = cell_at(&event) {
                selected.set_value(Some((row, column)));
                select_cell(scroll, &cell);
                open_menu(app, &event, cell_menu(app, &tab, &table, row, column));
            }
        }
    };
    let on_key = {
        let (table, details) = (table.clone(), tab.details.clone());
        move |event: web_sys::KeyboardEvent| {
            let Some((row, column)) = selected.get_value() else {
                return;
            };
            match event.key().as_str() {
                "c" | "C" if event.ctrl_key() || event.meta_key() => {
                    event.prevent_default();
                    let data = table.get();
                    let value = data
                        .rows
                        .get(row)
                        .and_then(|cells| cells.get(column))
                        .cloned()
                        .unwrap_or(Value::Null);
                    app.copy(format::display(&value, &data.columns[column].column_type), "the value");
                }
                "Enter" => {
                    event.prevent_default();
                    details.set(Some(row));
                }
                "Escape" => details.set(None),
                _ => {}
            }
        }
    };
    let each = {
        move || {
            let (start, end) = window.get();
            rows.with(|rows| rows[start.min(rows.len())..end.min(rows.len())].to_vec())
        }
    };
    let render_row = {
        let (table, numeric) = (table.clone(), Arc::clone(&numeric));
        move |row: usize| grid_row(&table, &numeric, row, selected.get_value())
    };
    let details = {
        let (tab, table) = (tab.clone(), table.clone());
        move || {
            tab.details
                .get()
                .filter(|row| *row < loaded)
                .map(|row| details_view(app, &tab, &table, row))
        }
    };
    let count = move || {
        let shown = rows.with(Vec::len);
        if shown == loaded {
            rows_label(total)
        } else {
            format!("{} of {}", time::format_count(shown as u64), rows_label(loaded))
        }
    };
    let columns = table.get().columns.len();
    view! {
        <div class="kx-grid-wrap">
            <div class="kx-grid" data-testid="grid" style=template>
                <div
                    class="kx-grid-scroll"
                    node_ref=scroll
                    tabindex="0"
                    role="grid"
                    aria-rowcount=loaded.to_string()
                    aria-colcount=columns.to_string()
                    on:scroll=move |event| {
                        if let Some(element) = event.target().and_then(|target| target.dyn_into::<web_sys::Element>().ok()) {
                            scroll_top.set(f64::from(element.scroll_top()));
                        }
                    }
                    on:keydown=on_key
                >
                    <div class="kx-grid-header" role="row">
                        <div class="kx-grid-gutter">"#"</div>
                        {headers}
                    </div>
                    <div
                        class="kx-grid-body"
                        style=move || format!("height: {}px", rows.with(Vec::len) as f64 * ROW_HEIGHT)
                        on:click=on_click
                        on:dblclick=on_double_click
                        on:contextmenu=on_menu
                    >
                        <div class="kx-grid-window" style=move || format!("transform: translateY({}px)", window.get().0 as f64 * ROW_HEIGHT)>
                            <For each=each key=|row| *row children=render_row/>
                        </div>
                    </div>
                    <Show when=move || rows.with(Vec::is_empty)>
                        <div class="kx-grid-empty">{if loaded == 0 { "The table has no rows." } else { "No rows match the search." }}</div>
                    </Show>
                </div>
                {details}
            </div>
            <div class="kx-grid-footer">
                <span data-testid="grid-count">{count}</span>
                {truncated
                    .then(|| {
                        view! {
                            <span class="kx-grid-truncated" data-testid="grid-truncated">
                                {icon("warning")}
                                {format!(
                                    "Showing the first {} of {}. Raise the row limit in Settings to see more.",
                                    time::format_count(loaded as u64),
                                    rows_label(total),
                                )}
                            </span>
                        }
                    })}
                <span class="kx-spacer"></span>
                <span class="kx-muted">{format!("{columns} columns")}</span>
                <span class="kx-muted">"Double-click a row for details; right-click for more"</span>
            </div>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, column_type: &str) -> ResultColumn {
        ResultColumn {
            name: name.to_owned(),
            column_type: column_type.to_owned(),
        }
    }

    #[test]
    fn small_tables_render_every_row() {
        assert_eq!(window_for(10, 500.0, 300.0), (0, 10));
        assert_eq!(window_for(VIRTUALIZE_ABOVE, 0.0, 300.0), (0, VIRTUALIZE_ABOVE));
    }

    #[test]
    fn large_tables_render_a_window_with_overscan() {
        assert_eq!(window_for(10_000, 240.0, 240.0), (0, 10 + 10 + OVERSCAN));
        let (start, end) = window_for(10_000, 24_000.0, 240.0);
        assert_eq!(start, 1_000 - OVERSCAN);
        assert_eq!(end, start + 10 + 2 * OVERSCAN);
        assert_eq!(window_for(1_000, 1e9, 240.0), (1_000, 1_000));
    }

    #[test]
    fn where_clauses_match_the_column_type() {
        assert_eq!(
            where_clause(&column("Name", "string"), &Value::from("a\"b"), true),
            "\n| where Name == \"a\\\"b\""
        );
        assert_eq!(
            where_clause(&column("Count", "long"), &Value::from(3), false),
            "\n| where Count != 3"
        );
        assert_eq!(
            where_clause(&column("Name", "string"), &Value::Null, true),
            "\n| where isempty(Name)"
        );
        assert_eq!(
            where_clause(&column("At", "datetime"), &Value::Null, false),
            "\n| where isnotnull(At)"
        );
        assert_eq!(
            where_clause(&column("my col", "dynamic"), &serde_json::json!({"a": 1}), true),
            "\n| where tostring([\"my col\"]) == \"{\\\"a\\\":1}\""
        );
    }

    #[test]
    fn long_text_is_truncated_on_char_boundaries() {
        assert_eq!(truncate("héllo", 2), "hé…");
        assert_eq!(truncate("hi", 5), "hi");
    }

    #[test]
    fn widths_fit_content_within_bounds() {
        let table = ResultTable {
            columns: vec![column("A", "string"), column("Description", "string")],
            rows: vec![vec![Value::from("x"), Value::from("y".repeat(200))]],
            ..ResultTable::default()
        };
        let widths = default_widths(&table);
        assert!((widths[0] - 56.0).abs() < f64::EPSILON);
        assert!(widths[1] > 400.0 && widths[1] <= 420.0);
        assert!((gutter_width(5) - 40.0).abs() < f64::EPSILON);
    }
}
