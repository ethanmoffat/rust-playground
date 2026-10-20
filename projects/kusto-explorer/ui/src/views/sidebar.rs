//! Side bar: connections tree, query history, starter queries, and settings.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use kusto_explorer_api::{
    ColumnSchema, ConnectionInfo, DEFAULT_ROW_CAP, DatabaseInfo, DatabaseSchema, MAX_ROW_CAP, QueryKind,
};
use leptos::prelude::*;
use wasm_bindgen::JsCast;

use super::{chrome::rows_label, icon, keys, open_menu};
use crate::{
    blocks,
    format::{kql_name, kql_string},
    keymap::Command,
    persist::{
        HistoryEntry, Layout, MAX_FONT_SIZE, MAX_SIDEBAR_WIDTH, MIN_FONT_SIZE, MIN_SIDEBAR_WIDTH, SideView, ThemeChoice,
    },
    starters::{STARTERS, Starter},
    state::{App, Health, KEY_SEPARATOR, Load, MenuEntry, NewTab, db_key},
    time,
};

const ROW_CAPS: [usize; 5] = [1_000, 10_000, DEFAULT_ROW_CAP, 100_000, MAX_ROW_CAP];

#[component]
pub(crate) fn Sidebar() -> impl IntoView {
    let app = expect_context::<App>();
    let visible = Memo::new(move |_| app.layout.with(|layout| layout.sidebar_visible));
    let width = Memo::new(move |_| app.layout.with(|layout| layout.sidebar_width));
    let side = Memo::new(move |_| app.layout.with(|layout| layout.view));
    let drag = StoredValue::new(None::<(f64, f64)>);
    let dragging = RwSignal::new(false);
    let stop = move || {
        drag.set_value(None);
        dragging.set(false);
    };
    view! {
        <Show when=move || visible.get()>
            <aside
                class="kx-sidebar"
                data-testid="sidebar"
                data-view=move || side.get().id()
                style=move || format!("width: {}px", width.get())
                aria-label=move || side.get().title()
            >
                <div class="kx-sidebar-header">
                    <span class="kx-sidebar-title">{move || side.get().title().to_uppercase()}</span>
                </div>
                <div class="kx-sidebar-body">
                    {move || match side.get() {
                        SideView::Connections => view! { <ConnectionsView/> }.into_any(),
                        SideView::History => view! { <HistoryView/> }.into_any(),
                        SideView::Starters => view! { <StartersView/> }.into_any(),
                        SideView::Settings => view! { <SettingsView/> }.into_any(),
                    }}
                </div>
            </aside>
            <div
                class="kx-sash kx-sash-vertical"
                class:active=move || dragging.get()
                role="separator"
                aria-orientation="vertical"
                title="Drag to resize; double-click to reset"
                on:pointerdown=move |event| {
                    if event.button() != 0 {
                        return;
                    }
                    event.prevent_default();
                    if let Some(target) = event.current_target().and_then(|target| target.dyn_into::<web_sys::Element>().ok()) {
                        let _ = target.set_pointer_capture(event.pointer_id());
                    }
                    drag.set_value(Some((f64::from(event.client_x()), width.get_untracked())));
                    dragging.set(true);
                }
                on:pointermove=move |event| {
                    let Some((start, initial)) = drag.get_value() else { return };
                    let width = (initial + f64::from(event.client_x()) - start).clamp(MIN_SIDEBAR_WIDTH, MAX_SIDEBAR_WIDTH);
                    app.layout.update(|layout| layout.sidebar_width = width);
                }
                on:pointerup=move |_| stop()
                on:lostpointercapture=move |_| stop()
                on:dblclick=move |_| app.layout.update(|layout| layout.sidebar_width = Layout::default().sidebar_width)
            ></div>
        </Show>
    }
}

/// Kinds of schema entities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum EntityKind {
    Table,
    MaterializedView,
    ExternalTable,
    Function,
}

impl EntityKind {
    pub(super) const ALL: [Self; 4] = [Self::Table, Self::MaterializedView, Self::ExternalTable, Self::Function];

    fn id(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::MaterializedView => "materialized-view",
            Self::ExternalTable => "external-table",
            Self::Function => "function",
        }
    }

    fn folder(self) -> &'static str {
        match self {
            Self::Table => "Tables",
            Self::MaterializedView => "Materialized views",
            Self::ExternalTable => "External tables",
            Self::Function => "Functions",
        }
    }

    pub(super) fn icon(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::MaterializedView => "layers",
            Self::ExternalTable => "cloud",
            Self::Function => "symbol-method",
        }
    }

    /// How a query refers to an entity of this kind.
    pub(super) fn reference(self, name: &str) -> String {
        match self {
            Self::Table | Self::MaterializedView => kql_name(name),
            Self::ExternalTable => format!("external_table({})", kql_string(name)),
            Self::Function => format!("{}()", kql_name(name)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum NoteKind {
    Loading,
    Error,
    Empty,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Node {
    Cluster {
        connection: String,
        uri: String,
    },
    Database {
        connection: String,
        database: String,
    },
    Folder {
        kind: EntityKind,
    },
    Entity {
        connection: String,
        database: String,
        kind: EntityKind,
        name: String,
    },
    Column {
        connection: String,
        database: String,
        kind: EntityKind,
        entity: String,
        name: String,
        column_type: String,
    },
    Note(NoteKind),
}

impl Node {
    fn data_kind(&self) -> &'static str {
        match self {
            Self::Cluster { .. } => "cluster",
            Self::Database { .. } => "database",
            Self::Folder { .. } => "folder",
            Self::Entity { kind, .. } => kind.id(),
            Self::Column { .. } => "column",
            Self::Note(_) => "note",
        }
    }

    fn icon(&self, expanded: Option<bool>) -> &'static str {
        match self {
            Self::Cluster { .. } => "server",
            Self::Database { .. } => "database",
            Self::Folder { .. } if expanded == Some(true) => "folder-opened",
            Self::Folder { .. } => "folder",
            Self::Entity { kind, .. } => kind.icon(),
            Self::Column { column_type, .. } => column_icon(column_type),
            Self::Note(NoteKind::Loading) => "loading codicon-modifier-spin",
            Self::Note(NoteKind::Error) => "warning",
            Self::Note(NoteKind::Empty) => "info",
        }
    }
}

fn column_icon(column_type: &str) -> &'static str {
    match column_type.to_ascii_lowercase().as_str() {
        "string" => "symbol-string",
        "int" | "long" | "real" | "double" | "decimal" => "symbol-number",
        "bool" | "boolean" => "symbol-boolean",
        "datetime" | "date" => "calendar",
        "timespan" | "time" => "watch",
        "dynamic" => "bracket",
        "guid" | "uniqueid" => "symbol-key",
        _ => "symbol-field",
    }
}

/// One visible line of the connections tree.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Row {
    key: String,
    depth: u16,
    node: Node,
    label: String,
    detail: String,
    tooltip: String,
    expanded: Option<bool>,
    current: bool,
    status: Option<&'static str>,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Tree {
    rows: Vec<Row>,
    /// Expanded databases whose schema has not been requested yet.
    wanted: Vec<(String, String)>,
}

impl Tree {
    fn note(&mut self, depth: u16, parent: &str, kind: NoteKind, text: &str) {
        self.rows.push(Row {
            key: format!("n:{parent}"),
            depth,
            node: Node::Note(kind),
            label: text.to_owned(),
            detail: String::new(),
            tooltip: text.to_owned(),
            expanded: None,
            current: false,
            status: None,
        });
    }
}

struct TreeInput<'a> {
    connections: &'a [ConnectionInfo],
    defaults: &'a HashMap<String, String>,
    health: &'a HashMap<String, Option<bool>>,
    databases: &'a HashMap<String, Load<Arc<Vec<DatabaseInfo>>>>,
    schemas: &'a HashMap<String, Load<Arc<DatabaseSchema>>>,
    toggled: &'a BTreeSet<String>,
    filter: &'a str,
    current: (&'a str, &'a str),
}

/// Expansion state: the default flipped by user toggles.
fn is_open(key: &str, default: bool, toggled: &BTreeSet<String>) -> bool {
    default != toggled.contains(key)
}

fn host_of(uri: &str) -> String {
    uri.split_once("://")
        .map_or(uri, |(_, rest)| rest)
        .trim_end_matches('/')
        .to_owned()
}

struct Item<'a> {
    name: &'a str,
    doc: Option<&'a str>,
    columns: &'a [ColumnSchema],
    parameters: Option<&'a str>,
}

fn items(schema: &DatabaseSchema, kind: EntityKind) -> Vec<Item<'_>> {
    let mut items: Vec<Item<'_>> = match kind {
        EntityKind::Table => &schema.tables,
        EntityKind::MaterializedView => &schema.materialized_views,
        EntityKind::ExternalTable => &schema.external_tables,
        EntityKind::Function => {
            return sorted(
                schema
                    .functions
                    .iter()
                    .map(|function| Item {
                        name: &function.name,
                        doc: function.doc_string.as_deref(),
                        columns: &[],
                        parameters: Some(&function.parameters),
                    })
                    .collect(),
            );
        }
    }
    .iter()
    .map(|entity| Item {
        name: &entity.name,
        doc: entity.doc_string.as_deref(),
        columns: &entity.columns,
        parameters: None,
    })
    .collect();
    items.sort_by_cached_key(|item| item.name.to_lowercase());
    items
}

fn sorted(mut items: Vec<Item<'_>>) -> Vec<Item<'_>> {
    items.sort_by_cached_key(|item| item.name.to_lowercase());
    items
}

fn contains(text: &str, filter: &str) -> bool {
    text.to_lowercase().contains(filter)
}

fn schema_matches(schema: &DatabaseSchema, filter: &str) -> bool {
    EntityKind::ALL.iter().any(|kind| {
        items(schema, *kind)
            .iter()
            .any(|item| contains(item.name, filter) || item.columns.iter().any(|column| contains(&column.name, filter)))
    })
}

fn build_tree(input: &TreeInput<'_>) -> Tree {
    let mut tree = Tree::default();
    let filter = input.filter.trim().to_lowercase();
    for connection in input.connections {
        let key = format!("c:{}", connection.id);
        let open = !filter.is_empty() || is_open(&key, true, input.toggled);
        let status = match input.health.get(&connection.id).copied().flatten() {
            Some(true) => "connected",
            Some(false) => "unreachable",
            None => "unknown",
        };
        tree.rows.push(Row {
            key: key.clone(),
            depth: 0,
            node: Node::Cluster {
                connection: connection.id.clone(),
                uri: connection.cluster_uri.clone(),
            },
            label: connection.name.clone(),
            detail: host_of(&connection.cluster_uri),
            tooltip: format!("{} ({})", connection.cluster_uri, status),
            expanded: Some(open),
            current: false,
            status: Some(status),
        });
        if !open {
            continue;
        }
        match input.databases.get(&connection.id) {
            None | Some(Load::Loading) => tree.note(1, &key, NoteKind::Loading, "Loading databases…"),
            Some(Load::Failed(error)) => tree.note(1, &key, NoteKind::Error, error),
            Some(Load::Ready(databases)) if databases.is_empty() => tree.note(1, &key, NoteKind::Empty, "No databases"),
            Some(Load::Ready(databases)) => {
                for database in databases.iter() {
                    add_database(&mut tree, input, connection, database, &filter);
                }
            }
        }
    }
    tree
}

fn add_database(
    tree: &mut Tree,
    input: &TreeInput<'_>,
    connection: &ConnectionInfo,
    database: &DatabaseInfo,
    filter: &str,
) {
    let name = &database.name;
    let database_key = db_key(&connection.id, name);
    let key = format!("d:{database_key}");
    let schema = input.schemas.get(&database_key);
    let searching = !filter.is_empty() && !contains(name, filter);
    if searching && !matches!(schema, Some(Load::Ready(schema)) if schema_matches(schema, filter)) {
        return;
    }
    let default_open = input.defaults.get(&connection.id) == Some(name);
    let open = searching || is_open(&key, default_open, input.toggled);
    let current = input.current == (connection.id.as_str(), name.as_str());
    let pretty = database
        .pretty_name
        .clone()
        .filter(|pretty| !pretty.is_empty() && pretty != name);
    tree.rows.push(Row {
        key: key.clone(),
        depth: 1,
        node: Node::Database {
            connection: connection.id.clone(),
            database: name.clone(),
        },
        label: name.clone(),
        detail: pretty.clone().unwrap_or_default(),
        tooltip: if current {
            format!("{name} (used by the current tab)")
        } else {
            format!("{name}: click to use it in the current tab")
        },
        expanded: Some(open),
        current,
        status: None,
    });
    if !open {
        return;
    }
    match schema {
        None => {
            tree.wanted.push((connection.id.clone(), name.clone()));
            tree.note(2, &key, NoteKind::Loading, "Loading schema…");
        }
        Some(Load::Loading) => tree.note(2, &key, NoteKind::Loading, "Loading schema…"),
        Some(Load::Failed(error)) => tree.note(2, &key, NoteKind::Error, error),
        Some(Load::Ready(schema)) => add_folders(
            tree,
            input,
            connection,
            name,
            schema,
            if searching { filter } else { "" },
        ),
    }
}

fn add_folders(
    tree: &mut Tree,
    input: &TreeInput<'_>,
    connection: &ConnectionInfo,
    database: &str,
    schema: &DatabaseSchema,
    filter: &str,
) {
    let database_key = db_key(&connection.id, database);
    for kind in EntityKind::ALL {
        let matches: Vec<(Item<'_>, Option<Vec<&ColumnSchema>>)> = items(schema, kind)
            .into_iter()
            .filter_map(|item| {
                if filter.is_empty() || contains(item.name, filter) {
                    return Some((item, None));
                }
                let columns: Vec<&ColumnSchema> = item
                    .columns
                    .iter()
                    .filter(|column| contains(&column.name, filter))
                    .collect();
                (!columns.is_empty()).then_some((item, Some(columns)))
            })
            .collect();
        if matches.is_empty() && (kind != EntityKind::Table || !filter.is_empty()) {
            continue;
        }
        let folder_key = format!("f:{database_key}{KEY_SEPARATOR}{}", kind.id());
        let open = !filter.is_empty() || is_open(&folder_key, kind == EntityKind::Table, input.toggled);
        tree.rows.push(Row {
            key: folder_key.clone(),
            depth: 2,
            node: Node::Folder { kind },
            label: kind.folder().to_owned(),
            detail: matches.len().to_string(),
            tooltip: format!("{} {}", matches.len(), kind.folder().to_lowercase()),
            expanded: Some(open),
            current: false,
            status: None,
        });
        if !open {
            continue;
        }
        if matches.is_empty() {
            tree.note(3, &folder_key, NoteKind::Empty, "No tables");
            continue;
        }
        for (item, columns) in matches {
            add_entity(tree, input, connection, database, kind, &item, columns);
        }
    }
}

fn add_entity(
    tree: &mut Tree,
    input: &TreeInput<'_>,
    connection: &ConnectionInfo,
    database: &str,
    kind: EntityKind,
    item: &Item<'_>,
    columns: Option<Vec<&ColumnSchema>>,
) {
    let database_key = db_key(&connection.id, database);
    let key = format!(
        "e:{database_key}{KEY_SEPARATOR}{}{KEY_SEPARATOR}{}",
        kind.id(),
        item.name
    );
    let expandable = !item.columns.is_empty();
    let open = columns.is_some() || (expandable && is_open(&key, false, input.toggled));
    let mut tooltip = match item.parameters {
        Some(parameters) => format!("{}{parameters}", item.name),
        None => format!("{} ({} columns)", item.name, item.columns.len()),
    };
    if let Some(doc) = item.doc.filter(|doc| !doc.is_empty()) {
        tooltip = format!("{tooltip}\n{doc}");
    }
    tree.rows.push(Row {
        key: key.clone(),
        depth: 3,
        node: Node::Entity {
            connection: connection.id.clone(),
            database: database.to_owned(),
            kind,
            name: item.name.to_owned(),
        },
        label: item.name.to_owned(),
        detail: item.parameters.unwrap_or_default().to_owned(),
        tooltip,
        expanded: expandable.then_some(open),
        current: false,
        status: None,
    });
    if !open {
        return;
    }
    let columns = columns.unwrap_or_else(|| item.columns.iter().collect());
    for column in columns {
        let mut tooltip = format!("{}: {}", column.name, column.column_type);
        if let Some(doc) = column.doc_string.as_deref().filter(|doc| !doc.is_empty()) {
            tooltip = format!("{tooltip}\n{doc}");
        }
        tree.rows.push(Row {
            key: format!("k:{key}{KEY_SEPARATOR}{}", column.name),
            depth: 4,
            node: Node::Column {
                connection: connection.id.clone(),
                database: database.to_owned(),
                kind,
                entity: item.name.to_owned(),
                name: column.name.clone(),
                column_type: column.column_type.clone(),
            },
            label: column.name.clone(),
            detail: column.column_type.clone(),
            tooltip,
            expanded: None,
            current: false,
            status: None,
        });
    }
}

fn flip(app: App, key: &str) {
    app.toggled.update(|toggled| {
        if !toggled.remove(key) {
            toggled.insert(key.to_owned());
        }
    });
}

pub(super) fn query_tab(app: App, title: String, text: String, connection: &str, database: &str, run: bool) {
    app.open_tab(NewTab {
        title: Some(title).filter(|title| !title.is_empty()),
        text,
        connection: Some(connection.to_owned()),
        database: Some(database.to_owned()).filter(|database| !database.is_empty()),
        run,
        ..NewTab::default()
    });
}

fn current_database(app: App, connection: &str) -> String {
    app.current_tab()
        .filter(|tab| tab.connection.get_untracked() == connection)
        .map(|tab| tab.database.get_untracked())
        .filter(|database| !database.is_empty())
        .or_else(|| {
            app.connection_info(connection)
                .and_then(|info| app.default_database(&info))
        })
        .unwrap_or_default()
}

fn node_menu(app: App, node: &Node) -> Vec<MenuEntry> {
    match node.clone() {
        Node::Cluster { connection, uri } => {
            let (c1, c2, c3) = (connection.clone(), connection.clone(), connection);
            vec![
                MenuEntry::item("New query tab", "add", move || {
                    app.open_tab(NewTab {
                        connection: Some(c1.clone()),
                        ..NewTab::default()
                    });
                }),
                MenuEntry::item("Show databases", "database", move || {
                    query_tab(
                        app,
                        "Databases".to_owned(),
                        ".show databases".to_owned(),
                        &c2,
                        &current_database(app, &c2),
                        true,
                    );
                }),
                MenuEntry::item("Show version", "info", move || {
                    query_tab(
                        app,
                        "Version".to_owned(),
                        ".show version".to_owned(),
                        &c3,
                        &current_database(app, &c3),
                        true,
                    );
                }),
                MenuEntry::item("Refresh", "refresh", move || app.refresh()),
                MenuEntry::Separator,
                MenuEntry::item("Copy cluster URI", "copy", move || {
                    app.copy(uri.clone(), "the cluster URI")
                }),
            ]
        }
        Node::Database { connection, database } => {
            let tab = (connection.clone(), database.clone());
            let used = (connection.clone(), database.clone());
            let tables = (connection.clone(), database.clone());
            let counts = (connection.clone(), database.clone());
            let schema = (connection, database.clone());
            vec![
                MenuEntry::item("New query tab", "add", move || {
                    query_tab(app, String::new(), String::new(), &tab.0, &tab.1, false)
                }),
                MenuEntry::item("Use in current tab", "check", move || {
                    app.use_database(&used.0, &used.1)
                }),
                MenuEntry::Separator,
                MenuEntry::item("Show tables", "table", move || {
                    query_tab(
                        app,
                        format!("{} tables", tables.1),
                        ".show tables".to_owned(),
                        &tables.0,
                        &tables.1,
                        true,
                    );
                }),
                MenuEntry::item("Row count per table", "graph", move || {
                    let query = "union withsource=TableName *\n| summarize Rows = count() by TableName\n| order by TableName asc";
                    query_tab(
                        app,
                        format!("{} rows", counts.1),
                        query.to_owned(),
                        &counts.0,
                        &counts.1,
                        true,
                    );
                }),
                MenuEntry::item("Show schema", "symbol-structure", move || {
                    let query = format!(".show database {} schema", kql_name(&schema.1));
                    query_tab(app, format!("{} schema", schema.1), query, &schema.0, &schema.1, true);
                }),
                MenuEntry::Separator,
                MenuEntry::item("Copy name", "copy", move || {
                    app.copy(database.clone(), "the database name")
                }),
            ]
        }
        Node::Entity {
            connection,
            database,
            kind,
            name,
        } => entity_menu(app, connection, database, kind, name),
        Node::Column {
            connection,
            database,
            kind,
            entity,
            name,
            ..
        } => {
            let mut entries = Vec::new();
            if kind != EntityKind::Function {
                let (connection, database, entity, name) = (connection, database, entity, name.clone());
                entries.push(MenuEntry::item("Top values", "graph", move || {
                    let query = format!(
                        "{}\n| summarize Count = count() by {}\n| top 20 by Count desc",
                        kind.reference(&entity),
                        kql_name(&name)
                    );
                    query_tab(app, format!("{entity}.{name}"), query, &connection, &database, true);
                }));
                entries.push(MenuEntry::Separator);
            }
            let insert = kql_name(&name);
            entries.push(MenuEntry::item("Insert name", "edit", move || app.insert(&insert)));
            entries.push(MenuEntry::item("Copy name", "copy", move || {
                app.copy(name.clone(), "the column name")
            }));
            entries
        }
        Node::Folder { .. } | Node::Note(_) => Vec::new(),
    }
}

fn entity_menu(app: App, connection: String, database: String, kind: EntityKind, name: String) -> Vec<MenuEntry> {
    let reference = kind.reference(&name);
    let run = move |title: String, query: String| {
        let (connection, database) = (connection.clone(), database.clone());
        move || query_tab(app, title.clone(), query.clone(), &connection, &database, true)
    };
    let mut entries = Vec::new();
    if kind == EntityKind::Function {
        entries.push(MenuEntry::item(
            "Show definition",
            "code",
            run(name.clone(), format!(".show function {}", kql_name(&name))),
        ));
    } else {
        entries.push(MenuEntry::item(
            "Preview 100 rows",
            "play",
            run(name.clone(), format!("{reference}\n| take 100")),
        ));
        entries.push(MenuEntry::item(
            "Count rows",
            "symbol-number",
            run(format!("{name} count"), format!("{reference}\n| count")),
        ));
        entries.push(MenuEntry::item(
            "Show schema",
            "symbol-structure",
            run(format!("{name} schema"), format!("{reference}\n| getschema")),
        ));
        match kind {
            EntityKind::Table => entries.push(MenuEntry::item(
                "Show extents",
                "layers",
                run(
                    format!("{name} extents"),
                    format!(".show table {} extents", kql_name(&name)),
                ),
            )),
            EntityKind::MaterializedView => entries.push(MenuEntry::item(
                "Show materialized view",
                "info",
                run(name.clone(), format!(".show materialized-view {}", kql_name(&name))),
            )),
            EntityKind::ExternalTable | EntityKind::Function => {}
        }
    }
    entries.push(MenuEntry::Separator);
    entries.push(MenuEntry::item("Insert into query", "edit", move || {
        app.insert(&reference)
    }));
    entries.push(MenuEntry::item("Copy name", "copy", move || {
        app.copy(name.clone(), "the name")
    }));
    entries
}

fn tree_row(app: App, row: Row) -> AnyView {
    let Row {
        key,
        depth,
        node,
        label,
        detail,
        tooltip,
        expanded,
        current,
        status,
    } = row;
    let indent = format!("padding-left: {}px", 6 + u32::from(depth) * 12);
    let twistie = {
        let key = key.clone();
        move |event: web_sys::MouseEvent| {
            if expanded.is_some() {
                event.stop_propagation();
                flip(app, &key);
            }
        }
    };
    let on_click = {
        let (node, key) = (node.clone(), key.clone());
        move |_| match &node {
            Node::Database { connection, database } => {
                app.use_database(connection, database);
                if expanded == Some(false) {
                    flip(app, &key);
                }
            }
            _ if expanded.is_some() => flip(app, &key),
            _ => {}
        }
    };
    let on_double_click = {
        let node = node.clone();
        move |_| match &node {
            Node::Entity { kind, name, .. } => app.insert(&kind.reference(name)),
            Node::Column { name, .. } => app.insert(&kql_name(name)),
            _ => {}
        }
    };
    let on_menu = {
        let node = node.clone();
        move |event: web_sys::MouseEvent| {
            let entries = node_menu(app, &node);
            if !entries.is_empty() {
                open_menu(app, &event, entries);
            }
        }
    };
    let action = match &node {
        Node::Entity {
            connection,
            database,
            kind,
            name,
        } if *kind != EntityKind::Function => {
            let (connection, database, query) = (
                connection.clone(),
                database.clone(),
                format!("{}\n| take 100", kind.reference(name)),
            );
            let title = name.clone();
            Some(
                view! {
                    <button
                        class="kx-tree-action"
                        title="Preview 100 rows in a new tab"
                        aria-label="Preview 100 rows"
                        on:click=move |event| {
                            event.stop_propagation();
                            query_tab(app, title.clone(), query.clone(), &connection, &database, true);
                        }
                    >
                        {icon("play")}
                    </button>
                }
                .into_any(),
            )
        }
        Node::Database { connection, database } => {
            let (connection, database) = (connection.clone(), database.clone());
            Some(
                view! {
                    <button
                        class="kx-tree-action"
                        title="New query tab for this database"
                        aria-label="New query tab"
                        on:click=move |event| {
                            event.stop_propagation();
                            query_tab(app, String::new(), String::new(), &connection, &database, false);
                        }
                    >
                        {icon("add")}
                    </button>
                }
                .into_any(),
            )
        }
        Node::Cluster { .. } => Some(
            view! {
                <button
                    class="kx-tree-action"
                    title="Refresh databases and schemas"
                    aria-label="Refresh"
                    on:click=move |event| {
                        event.stop_propagation();
                        app.refresh();
                    }
                >
                    {icon("refresh")}
                </button>
            }
            .into_any(),
        ),
        _ => None,
    };
    let data_name = label.clone();
    view! {
        <div
            class="kx-tree-row"
            class:current=current
            class:note=matches!(node, Node::Note(_))
            role="treeitem"
            aria-expanded=expanded.map(|open| open.to_string())
            data-testid="tree-node"
            data-kind=node.data_kind()
            data-name=data_name
            title=tooltip
            style=indent
            on:click=on_click
            on:dblclick=on_double_click
            on:contextmenu=on_menu
        >
            <span class="kx-twistie" on:click=twistie>
                {expanded.map(|open| icon(if open { "chevron-down" } else { "chevron-right" }))}
            </span>
            {icon(node.icon(expanded))}
            <span class="kx-tree-label">{label}</span>
            {status.map(|state| view! { <span class="kx-tree-status" data-state=state></span> })}
            {current.then(|| icon("check"))}
            <span class="kx-tree-detail">{detail}</span>
            {action}
        </div>
    }
    .into_any()
}

#[component]
fn ConnectionsView() -> impl IntoView {
    let app = expect_context::<App>();
    let current = Memo::new(move |_| {
        app.active_tab()
            .map(|tab| (tab.connection.get(), tab.database.get()))
            .unwrap_or_default()
    });
    let tree = Memo::new(move |_| {
        let connections = app.config.with(|config| {
            config
                .as_ref()
                .map(|config| config.connections.clone())
                .unwrap_or_default()
        });
        app.databases.track();
        let defaults: HashMap<String, String> = connections
            .iter()
            .filter_map(|connection| {
                app.default_database(connection)
                    .map(|database| (connection.id.clone(), database))
            })
            .collect();
        let health: HashMap<String, Option<bool>> = app.health.with(|health| {
            connections
                .iter()
                .map(|connection| (connection.id.clone(), health.connection(&connection.id)))
                .collect()
        });
        let (connection, database) = current.get();
        let filter = app.tree_filter.get();
        app.databases.with(|databases| {
            app.schemas.with(|schemas| {
                app.toggled.with(|toggled| {
                    build_tree(&TreeInput {
                        connections: &connections,
                        defaults: &defaults,
                        health: &health,
                        databases,
                        schemas,
                        toggled,
                        filter: &filter,
                        current: (&connection, &database),
                    })
                })
            })
        })
    });
    Effect::new(move |_| {
        for (connection, database) in tree.with(|tree| tree.wanted.clone()) {
            if !app
                .schemas
                .with_untracked(|schemas| schemas.contains_key(&db_key(&connection, &database)))
            {
                app.load_schema(&connection, &database);
            }
        }
    });
    let collapse_all = move |_| {
        let open: Vec<String> = tree.with_untracked(|tree| {
            tree.rows
                .iter()
                .filter(|row| row.expanded == Some(true))
                .map(|row| row.key.clone())
                .collect()
        });
        app.toggled.update(|toggled| {
            for key in open {
                if !toggled.remove(&key) {
                    toggled.insert(key);
                }
            }
        });
    };
    view! {
        <div class="kx-pane-toolbar">
            <label class="kx-search">
                {icon("filter")}
                <input
                    type="search"
                    data-testid="tree-filter"
                    placeholder="Filter tables and columns"
                    aria-label="Filter tables and columns"
                    prop:value=move || app.tree_filter.get()
                    on:input=move |event| app.tree_filter.set(event_target_value(&event))
                />
            </label>
            <button class="kx-icon-button" title="Collapse all" aria-label="Collapse all" on:click=collapse_all>
                {icon("collapse-all")}
            </button>
            <button class="kx-icon-button" title="Refresh databases and schemas" aria-label="Refresh" on:click=move |_| app.refresh()>
                {icon("refresh")}
            </button>
        </div>
        {move || {
            app.config_error
                .get()
                .map(|error| {
                    view! {
                        <div class="kx-callout kx-callout-error">
                            {icon("error")}
                            <span>{format!("Could not load the configuration: {error}")}</span>
                            <button class="kx-link" on:click=move |_| app.load_config()>"Retry"</button>
                        </div>
                    }
                })
        }}
        <div class="kx-tree" role="tree" aria-label="Connections" data-testid="connections-tree">
            <For each=move || tree.with(|tree| tree.rows.clone()) key=Row::clone children=move |row| tree_row(app, row)/>
        </div>
    }
}

fn history_item(app: App, entry: HistoryEntry) -> AnyView {
    let summary = blocks::summary(&entry.query, 160);
    let outcome = if entry.ok {
        rows_label(entry.rows)
    } else {
        "failed".to_owned()
    };
    let meta = format!(
        "{} · {} · {}",
        entry.database,
        outcome,
        time::format_duration(entry.duration_ms)
    );
    let at = entry.at;
    let tooltip = match &entry.error {
        Some(error) => format!("{error}\n\n{}", entry.query),
        None => entry.query.clone(),
    };
    let open = {
        let entry = entry.clone();
        move |run: bool| {
            query_tab(
                app,
                String::new(),
                entry.query.clone(),
                &entry.connection_id,
                &entry.database,
                run,
            );
        }
    };
    let menu = {
        let (entry, open) = (entry.clone(), open.clone());
        move |event: web_sys::MouseEvent| {
            let (open_tab, run_tab) = (open.clone(), open.clone());
            let (insert, copy, id) = (entry.query.clone(), entry.query.clone(), entry.id.clone());
            open_menu(
                app,
                &event,
                vec![
                    MenuEntry::item("Open in new tab", "go-to-file", move || open_tab(false)),
                    MenuEntry::item("Run in new tab", "play", move || run_tab(true)),
                    MenuEntry::item("Insert into query", "edit", move || app.insert(&insert)),
                    MenuEntry::item("Copy query", "copy", move || app.copy(copy.clone(), "the query")),
                    MenuEntry::Separator,
                    MenuEntry::item("Remove from history", "trash", move || {
                        app.history.update(|history| history.retain(|item| item.id != id))
                    }),
                ],
            );
        }
    };
    let command = entry.kind == QueryKind::Command;
    view! {
        <li class="kx-history-item" class:failed=!entry.ok data-testid="history-item" title=tooltip on:click=move |_| open(false) on:contextmenu=menu>
            {icon(if entry.ok { "pass-filled" } else { "error" })}
            <div class="kx-history-text">
                <code class="kx-history-query">{command.then(|| icon("terminal"))}{summary}</code>
                <span class="kx-history-meta">{meta} " · " {move || time::format_ago(at, app.clock.get())}</span>
            </div>
        </li>
    }
    .into_any()
}

#[component]
fn HistoryView() -> impl IntoView {
    let app = expect_context::<App>();
    let entries = Memo::new(move |_| {
        let filter = app.history_filter.get().trim().to_lowercase();
        app.history.with(|history| {
            history
                .iter()
                .filter(|entry| {
                    filter.is_empty() || contains(&entry.query, &filter) || contains(&entry.database, &filter)
                })
                .cloned()
                .collect::<Vec<_>>()
        })
    });
    let total = Memo::new(move |_| app.history.with(Vec::len));
    view! {
        <div class="kx-pane-toolbar">
            <label class="kx-search">
                {icon("filter")}
                <input
                    type="search"
                    data-testid="history-filter"
                    placeholder="Filter history"
                    aria-label="Filter history"
                    prop:value=move || app.history_filter.get()
                    on:input=move |event| app.history_filter.set(event_target_value(&event))
                />
            </label>
            <button
                class="kx-icon-button"
                data-testid="history-clear"
                title="Clear history"
                aria-label="Clear history"
                disabled=move || total.get() == 0
                on:click=move |_| app.dispatch(Command::ClearHistory)
            >
                {icon("clear-all")}
            </button>
        </div>
        <Show
            when=move || !entries.with(Vec::is_empty)
            fallback=move || {
                view! {
                    <div class="kx-empty">
                        {icon("history")}
                        <p>
                            {move || {
                                if total.get() == 0 { "Queries you run appear here." } else { "No queries match the filter." }
                            }}
                        </p>
                    </div>
                }
            }
        >
            <ul class="kx-list" data-testid="history-list">
                <For each=move || entries.get() key=|entry| entry.id.clone() children=move |entry| history_item(app, entry)/>
            </ul>
        </Show>
    }
}

fn starter_item(app: App, starter: &'static Starter) -> AnyView {
    let open = move |run: bool| {
        app.open_tab(NewTab {
            title: Some(starter.title.to_owned()),
            text: starter.query.to_owned(),
            run,
            ..NewTab::default()
        });
    };
    let menu = move |event: web_sys::MouseEvent| {
        open_menu(
            app,
            &event,
            vec![
                MenuEntry::item("Run in new tab", "play", move || open(true)),
                MenuEntry::item("Open in new tab", "go-to-file", move || open(false)),
                MenuEntry::item("Insert into query", "edit", move || app.insert(starter.query)),
                MenuEntry::item("Copy query", "copy", move || {
                    app.copy(starter.query.to_owned(), "the query")
                }),
            ],
        );
    };
    view! {
        <li
            class="kx-starter"
            data-testid="starter"
            data-id=starter.id
            title=starter.query
            on:click=move |_| open(true)
            on:contextmenu=menu
        >
            {icon(starter.icon)}
            <div class="kx-starter-text">
                <span class="kx-starter-title">{starter.title}</span>
                <span class="kx-starter-description">{starter.description}</span>
            </div>
        </li>
    }
    .into_any()
}

#[component]
fn StartersView() -> impl IntoView {
    let app = expect_context::<App>();
    let mut groups: Vec<(&'static str, Vec<&'static Starter>)> = Vec::new();
    for starter in STARTERS {
        match groups.iter_mut().find(|(group, _)| *group == starter.group) {
            Some((_, items)) => items.push(starter),
            None => groups.push((starter.group, vec![starter])),
        }
    }
    view! {
        <p class="kx-pane-hint">"Click a query to run it in a new tab against the current database. Right-click for more."</p>
        {groups
            .into_iter()
            .map(|(group, starters)| {
                view! {
                    <section class="kx-group">
                        <h3 class="kx-group-title">{group}</h3>
                        <ul class="kx-list">{starters.into_iter().map(|starter| starter_item(app, starter)).collect_view()}</ul>
                    </section>
                }
            })
            .collect_view()}
    }
}

#[component]
fn SettingsView() -> impl IntoView {
    let app = expect_context::<App>();
    let theme = Memo::new(move |_| app.prefs.with(|prefs| prefs.theme));
    let font_size = Memo::new(move |_| app.prefs.with(|prefs| prefs.font_size));
    let row_cap = Memo::new(move |_| app.prefs.with(|prefs| prefs.row_cap));
    let theme_button = move |choice: ThemeChoice, label: &'static str, icon_name: &'static str, id: &'static str| {
        view! {
            <button
                class="kx-segment"
                class:active=move || theme.get() == choice
                aria-pressed=move || (theme.get() == choice).to_string()
                data-testid=id
                on:click=move |_| app.prefs.update(|prefs| prefs.theme = choice)
            >
                {icon(icon_name)}
                <span>{label}</span>
            </button>
        }
    };
    let connections = move || {
        let connections = app.config.with(|config| {
            config
                .as_ref()
                .map(|config| config.connections.clone())
                .unwrap_or_default()
        });
        let health = app.health.get();
        connections
            .into_iter()
            .map(|connection| {
                let detail = match &health {
                    Health::Checked(readiness) => readiness
                        .connections
                        .iter()
                        .find(|item| item.id == connection.id)
                        .cloned(),
                    _ => None,
                };
                let (state, text) = match (&health, &detail) {
                    (_, Some(item)) if item.reachable => (
                        "connected",
                        item.version
                            .clone()
                            .map_or_else(|| "Connected".to_owned(), |version| format!("Connected · {version}")),
                    ),
                    (_, Some(item)) => (
                        "unreachable",
                        item.error.clone().unwrap_or_else(|| "Unreachable".to_owned()),
                    ),
                    (Health::ServerDown(error), None) => ("unreachable", error.clone()),
                    _ => ("unknown", "Checking…".to_owned()),
                };
                view! {
                    <li class="kx-setting-connection" data-testid="settings-connection">
                        <span class="kx-tree-status" data-state=state></span>
                        <div>
                            <div class="kx-setting-connection-name">{connection.name}</div>
                            <code class="kx-setting-connection-uri">{connection.cluster_uri}</code>
                            <div class="kx-setting-connection-state">{text}</div>
                        </div>
                    </li>
                }
            })
            .collect_view()
    };
    let version = move || {
        app.config
            .with(|config| config.as_ref().map(|config| config.version.clone()).unwrap_or_default())
    };
    view! {
        <div class="kx-settings">
            <section class="kx-setting">
                <h3>"Theme"</h3>
                <div class="kx-segments" role="group" aria-label="Theme">
                    {theme_button(ThemeChoice::Dark, "Dark", "color-mode", "theme-dark")}
                    {theme_button(ThemeChoice::Light, "Light", "eye", "theme-light")}
                    {theme_button(ThemeChoice::System, "System", "settings-gear", "theme-system")}
                </div>
            </section>
            <section class="kx-setting">
                <h3>"Editor font size"</h3>
                <div class="kx-stepper">
                    <button
                        class="kx-icon-button"
                        title="Smaller (Ctrl+-)"
                        aria-label="Smaller"
                        disabled=move || font_size.get() <= MIN_FONT_SIZE
                        on:click=move |_| app.dispatch(Command::ZoomOut)
                    >
                        {icon("zoom-out")}
                    </button>
                    <span class="kx-stepper-value">{move || format!("{} px", font_size.get())}</span>
                    <button
                        class="kx-icon-button"
                        title="Larger (Ctrl+=)"
                        aria-label="Larger"
                        disabled={move || font_size.get() >= MAX_FONT_SIZE}
                        on:click=move |_| app.dispatch(Command::ZoomIn)
                    >
                        {icon("zoom-in")}
                    </button>
                    <button class="kx-link" on:click=move |_| app.dispatch(Command::ZoomReset)>"Reset"</button>
                </div>
            </section>
            <section class="kx-setting">
                <h3>"Row limit per result table"</h3>
                <select
                    class="kx-select-input"
                    data-testid="row-cap"
                    aria-label="Row limit per result table"
                    on:change=move |event| {
                        if let Ok(cap) = event_target_value(&event).parse::<usize>() {
                            app.prefs.update(|prefs| prefs.row_cap = cap);
                        }
                    }
                >
                    {ROW_CAPS
                        .into_iter()
                        .map(|cap| {
                            let label = if cap == DEFAULT_ROW_CAP {
                                format!("{} rows (default)", time::format_count(cap as u64))
                            } else {
                                format!("{} rows", time::format_count(cap as u64))
                            };
                            view! { <option value=cap.to_string() prop:selected=move || row_cap.get() == cap>{label}</option> }
                        })
                        .collect_view()}
                </select>
                <p class="kx-setting-hint">"Larger results are truncated in the browser; the full row count is still reported."</p>
            </section>
            <section class="kx-setting">
                <h3>"Layout and data"</h3>
                <div class="kx-setting-actions">
                    <button class="kx-button" on:click=move |_| app.dispatch(Command::ResetLayout)>
                        {icon("layout")}
                        <span>"Reset layout"</span>
                    </button>
                    <button class="kx-button" on:click=move |_| app.dispatch(Command::ClearHistory)>
                        {icon("clear-all")}
                        <span>"Clear history"</span>
                    </button>
                    <button class="kx-button" on:click=move |_| app.dispatch(Command::Shortcuts)>
                        {icon("keyboard")}
                        <span>"Keyboard shortcuts"</span>
                        {keys("Shift+F1")}
                    </button>
                </div>
            </section>
            <section class="kx-setting">
                <h3>"Connections"</h3>
                <p class="kx-setting-hint">"Clusters are configured on the server with KUSTO_EXPLORER_CONNECTIONS."</p>
                <ul class="kx-list">{connections}</ul>
            </section>
            <section class="kx-setting">
                <h3>"About"</h3>
                <p class="kx-setting-hint">
                    {move || format!("kusto-explorer {}", version())}
                    " · "
                    <a href="/THIRD_PARTY_NOTICES.txt" target="_blank" rel="noopener">"Third-party notices"</a>
                </p>
            </section>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use kusto_explorer_api::EntitySchema;

    use super::*;

    fn schema() -> DatabaseSchema {
        let column = |name: &str, column_type: &str| ColumnSchema {
            name: name.to_owned(),
            column_type: column_type.to_owned(),
            doc_string: None,
        };
        DatabaseSchema {
            connection_id: "c".to_owned(),
            cluster_uri: "http://kusto:8080".to_owned(),
            database: "db".to_owned(),
            tables: vec![
                EntitySchema {
                    name: "beta".to_owned(),
                    columns: vec![column("Id", "long"), column("Name", "string")],
                    ..EntitySchema::default()
                },
                EntitySchema {
                    name: "Alpha".to_owned(),
                    columns: vec![column("Timestamp", "datetime")],
                    ..EntitySchema::default()
                },
            ],
            ..DatabaseSchema::default()
        }
    }

    struct Fixture {
        connections: Vec<ConnectionInfo>,
        defaults: HashMap<String, String>,
        health: HashMap<String, Option<bool>>,
        databases: HashMap<String, Load<Arc<Vec<DatabaseInfo>>>>,
        schemas: HashMap<String, Load<Arc<DatabaseSchema>>>,
    }

    impl Fixture {
        fn new(with_schema: bool) -> Self {
            let connections = vec![ConnectionInfo {
                id: "c".to_owned(),
                name: "Local".to_owned(),
                cluster_uri: "http://kusto:8080".to_owned(),
                default_database: Some("db".to_owned()),
            }];
            let databases = HashMap::from([(
                "c".to_owned(),
                Load::Ready(Arc::new(vec![
                    DatabaseInfo {
                        name: "db".to_owned(),
                        pretty_name: None,
                    },
                    DatabaseInfo {
                        name: "other".to_owned(),
                        pretty_name: None,
                    },
                ])),
            )]);
            let mut schemas = HashMap::new();
            if with_schema {
                schemas.insert(db_key("c", "db"), Load::Ready(Arc::new(schema())));
            }
            Self {
                connections,
                defaults: HashMap::from([("c".to_owned(), "db".to_owned())]),
                health: HashMap::from([("c".to_owned(), Some(true))]),
                databases,
                schemas,
            }
        }

        fn build(&self, toggled: &BTreeSet<String>, filter: &str) -> Tree {
            build_tree(&TreeInput {
                connections: &self.connections,
                defaults: &self.defaults,
                health: &self.health,
                databases: &self.databases,
                schemas: &self.schemas,
                toggled,
                filter,
                current: ("c", "db"),
            })
        }
    }

    fn labels(tree: &Tree) -> Vec<(u16, &str)> {
        tree.rows.iter().map(|row| (row.depth, row.label.as_str())).collect()
    }

    #[test]
    fn default_database_and_tables_start_expanded() {
        let tree = Fixture::new(true).build(&BTreeSet::new(), "");
        assert_eq!(
            labels(&tree),
            vec![
                (0, "Local"),
                (1, "db"),
                (2, "Tables"),
                (3, "Alpha"),
                (3, "beta"),
                (1, "other")
            ]
        );
        assert!(tree.wanted.is_empty());
        assert!(tree.rows[1].current);
        assert_eq!(tree.rows[0].status, Some("connected"));
        assert_eq!(tree.rows[0].detail, "kusto:8080");
    }

    #[test]
    fn expanded_databases_without_schema_are_wanted() {
        let tree = Fixture::new(false).build(&BTreeSet::new(), "");
        assert_eq!(tree.wanted, vec![("c".to_owned(), "db".to_owned())]);
        assert_eq!(tree.rows[2].node, Node::Note(NoteKind::Loading));
    }

    #[test]
    fn toggles_flip_the_default() {
        let toggled = BTreeSet::from([
            format!("d:{}", db_key("c", "db")),
            format!("e:{}{KEY_SEPARATOR}table{KEY_SEPARATOR}beta", db_key("c", "db")),
        ]);
        let tree = Fixture::new(true).build(&toggled, "");
        assert_eq!(labels(&tree), vec![(0, "Local"), (1, "db"), (1, "other")]);
        let toggled = BTreeSet::from([format!(
            "e:{}{KEY_SEPARATOR}table{KEY_SEPARATOR}beta",
            db_key("c", "db")
        )]);
        let tree = Fixture::new(true).build(&toggled, "");
        assert_eq!(labels(&tree)[4..7], [(3, "beta"), (4, "Id"), (4, "Name")]);
    }

    #[test]
    fn filter_shows_matching_entities_and_columns() {
        let fixture = Fixture::new(true);
        let tree = fixture.build(&BTreeSet::new(), "time");
        assert_eq!(
            labels(&tree),
            vec![(0, "Local"), (1, "db"), (2, "Tables"), (3, "Alpha"), (4, "Timestamp")]
        );
        let tree = fixture.build(&BTreeSet::new(), "BET");
        assert_eq!(labels(&tree), vec![(0, "Local"), (1, "db"), (2, "Tables"), (3, "beta")]);
        let tree = fixture.build(&BTreeSet::new(), "oth");
        assert_eq!(labels(&tree), vec![(0, "Local"), (1, "other")]);
    }

    #[test]
    fn references_quote_when_needed() {
        assert_eq!(EntityKind::Table.reference("direct_logs"), "direct_logs");
        assert_eq!(EntityKind::ExternalTable.reference("ext"), "external_table(\"ext\")");
        assert_eq!(host_of("http://kusto:8080/"), "kusto:8080");
        assert_eq!(column_icon("datetime"), "calendar");
    }
}
