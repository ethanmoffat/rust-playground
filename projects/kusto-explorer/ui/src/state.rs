//! Application state and the actions that change it.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use kusto_explorer_api::{
    ApiError, AppConfig, CancelRequest, ConnectionInfo, DatabaseInfo, DatabaseSchema, QueryKind, QueryRequest,
    QueryResponse, Readiness, ResultTable, is_management_command, strip_leading_trivia,
};
use leptos::{prelude::*, task::spawn_local};

use crate::{
    api, blocks, browser,
    chart::ChartKind,
    deeplink::{self, SharedQuery},
    format::{self, Format},
    host,
    keymap::Command,
    persist::{
        HISTORY_KEY, HistoryEntry, Layout, MAX_FONT_SIZE, MIN_FONT_SIZE, Prefs, STATE_KEY, SavedState, SavedTab,
        SideView, ThemeChoice, push_history, restore_history,
    },
};

const WELCOME_QUERY: &str = "// Welcome! Press F5 or Shift+Enter to run the query at the cursor; blank lines separate queries.\n\
// Ctrl+Shift+P lists every command and Ctrl+P jumps to tables, tabs, and starter queries.\n\
union withsource=TableName *\n\
| summarize Rows = count() by TableName\n\
| order by TableName asc\n";
const HEALTH_INTERVAL: Duration = Duration::from_secs(20);
const SAVE_DELAY: Duration = Duration::from_millis(400);
const TOAST_LIFETIME: Duration = Duration::from_millis(4500);

/// Separator used in composite keys.
pub const KEY_SEPARATOR: char = '\u{1f}';

/// Key of a database within a connection.
#[must_use]
pub fn db_key(connection: &str, database: &str) -> String {
    format!("{connection}{KEY_SEPARATOR}{database}")
}

/// Asynchronously loaded data.
#[derive(Clone, Debug, PartialEq)]
pub enum Load<T> {
    /// Request in flight.
    Loading,
    /// Loaded.
    Ready(T),
    /// The request failed.
    Failed(String),
}

/// A finished query.
#[derive(Debug)]
pub struct Outcome {
    /// Response from the server.
    pub response: QueryResponse,
    /// Query text that ran.
    pub query: String,
    /// Database it ran in.
    pub database: String,
    /// Round-trip time in milliseconds.
    pub elapsed: f64,
    /// Completion time in epoch milliseconds.
    pub finished: f64,
}

/// A failed query.
#[derive(Debug)]
pub struct Failure {
    /// Error reported by the server.
    pub error: ApiError,
    /// Query text that failed.
    pub query: String,
    /// Round-trip time in milliseconds.
    pub elapsed: f64,
}

/// Execution state of a tab.
#[derive(Clone, Debug, Default)]
pub enum Run {
    /// Nothing ran yet.
    #[default]
    Idle,
    /// A request is in flight.
    Running {
        /// Client request identifier.
        request_id: String,
        /// Connection it runs on.
        connection: String,
        /// Start time in epoch milliseconds.
        started: f64,
        /// Whether it is a management command.
        command: bool,
    },
    /// Results arrived.
    Done(Arc<Outcome>),
    /// The request failed.
    Failed(Arc<Failure>),
    /// The user cancelled the request.
    Cancelled {
        /// Time until cancellation.
        elapsed: f64,
    },
}

impl Run {
    /// Whether a request is in flight.
    #[must_use]
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }
}

/// What the results panel shows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Pane {
    /// A result table.
    Table(usize),
    /// The chart.
    #[default]
    Chart,
    /// Statistics and messages.
    Stats,
}

/// A query tab.
#[derive(Clone, Debug)]
pub struct Tab {
    /// Identifier.
    pub id: u32,
    /// Title.
    pub title: ArcRwSignal<String>,
    /// Latest editor text.
    pub text: ArcRwSignal<String>,
    /// Connection identifier.
    pub connection: ArcRwSignal<String>,
    /// Database.
    pub database: ArcRwSignal<String>,
    /// File name after opening or saving a file.
    pub file_name: ArcRwSignal<Option<String>>,
    /// Execution state.
    pub run: ArcRwSignal<Run>,
    /// Visible results pane.
    pub pane: ArcRwSignal<Pane>,
    /// Chart kind chosen by the user.
    pub chart_kind: ArcRwSignal<Option<ChartKind>>,
    /// Grid filter text.
    pub filter: ArcRwSignal<String>,
    /// Grid sort as `(column, descending)`.
    pub sort: ArcRwSignal<Option<(usize, bool)>>,
    /// Column widths changed by the user.
    pub widths: ArcRwSignal<HashMap<usize, f64>>,
    /// Row shown in the details panel.
    pub details: ArcRwSignal<Option<usize>>,
}

impl PartialEq for Tab {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Tab {
    fn new(saved: SavedTab) -> Self {
        Self {
            id: saved.id,
            title: ArcRwSignal::new(saved.title),
            text: ArcRwSignal::new(saved.text),
            connection: ArcRwSignal::new(saved.connection_id),
            database: ArcRwSignal::new(saved.database),
            file_name: ArcRwSignal::new(saved.file_name),
            run: ArcRwSignal::new(Run::Idle),
            pane: ArcRwSignal::new(Pane::Table(0)),
            chart_kind: ArcRwSignal::new(None),
            filter: ArcRwSignal::new(String::new()),
            sort: ArcRwSignal::new(None),
            widths: ArcRwSignal::new(HashMap::new()),
            details: ArcRwSignal::new(None),
        }
    }

    /// Identifier used by the editor host.
    #[must_use]
    pub fn key(&self) -> String {
        self.id.to_string()
    }

    /// Clears the grid state after new results or a pane switch.
    pub fn reset_grid(&self) {
        self.filter.set(String::new());
        self.sort.set(None);
        self.widths.set(HashMap::new());
        self.details.set(None);
    }

    fn saved(&self) -> SavedTab {
        SavedTab {
            id: self.id,
            title: self.title.get(),
            text: self.text.get(),
            connection_id: self.connection.get(),
            database: self.database.get(),
            file_name: self.file_name.get(),
        }
    }
}

/// Modal overlays.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Overlay {
    /// None.
    #[default]
    None,
    /// Command palette or quick open, with the initial input.
    Palette(String),
    /// Keyboard shortcut sheet.
    Shortcuts,
}

/// An action in a context menu.
pub type MenuAction = Arc<dyn Fn() + Send + Sync>;

/// A context menu entry.
#[derive(Clone)]
pub enum MenuEntry {
    /// A command.
    Item {
        /// Label.
        label: String,
        /// Codicon name.
        icon: &'static str,
        /// Action.
        action: MenuAction,
    },
    /// A divider.
    Separator,
}

impl MenuEntry {
    /// A menu item.
    pub fn item(label: impl Into<String>, icon: &'static str, action: impl Fn() + Send + Sync + 'static) -> Self {
        Self::Item {
            label: label.into(),
            icon,
            action: Arc::new(action),
        }
    }
}

/// An open context menu.
#[derive(Clone)]
pub struct Menu {
    /// Left position in pixels.
    pub x: f64,
    /// Top position in pixels.
    pub y: f64,
    /// Entries.
    pub entries: Vec<MenuEntry>,
}

/// Toast severity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToastKind {
    /// Information.
    Info,
    /// Success.
    Success,
    /// Problem.
    Error,
}

/// A transient notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Toast {
    /// Identifier.
    pub id: u32,
    /// Severity.
    pub kind: ToastKind,
    /// Message.
    pub text: String,
}

/// Cluster reachability.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum Health {
    /// Not checked yet.
    #[default]
    Unknown,
    /// Result of the last check.
    Checked(Readiness),
    /// The kusto-explorer server did not answer.
    ServerDown(String),
}

impl Health {
    /// Reachability of one connection: `None` when unknown.
    #[must_use]
    pub fn connection(&self, id: &str) -> Option<bool> {
        match self {
            Self::Unknown => None,
            Self::Checked(readiness) => readiness
                .connections
                .iter()
                .find(|health| health.id == id)
                .map(|health| health.reachable),
            Self::ServerDown(_) => Some(false),
        }
    }
}

/// Editor cursor position.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cursor {
    /// Line, starting at 1.
    pub line: u32,
    /// Column, starting at 1.
    pub column: u32,
    /// Selected characters.
    pub selected: u32,
}

/// Options for a new tab; empty fields inherit from the active tab.
#[derive(Clone, Debug, Default)]
pub struct NewTab {
    /// Title.
    pub title: Option<String>,
    /// Query text.
    pub text: String,
    /// Connection identifier.
    pub connection: Option<String>,
    /// Database.
    pub database: Option<String>,
    /// File name.
    pub file_name: Option<String>,
    /// Whether to run the text immediately.
    pub run: bool,
}

/// The whole application state.
#[derive(Clone, Copy)]
pub struct App {
    /// Server configuration.
    pub config: RwSignal<Option<Arc<AppConfig>>>,
    /// Why the configuration could not be loaded.
    pub config_error: RwSignal<Option<String>>,
    /// Databases per connection.
    pub databases: RwSignal<HashMap<String, Load<Arc<Vec<DatabaseInfo>>>>>,
    /// Schemas per [`db_key`].
    pub schemas: RwSignal<HashMap<String, Load<Arc<DatabaseSchema>>>>,
    /// Tree nodes whose expansion differs from the default.
    pub toggled: RwSignal<BTreeSet<String>>,
    /// Connections tree filter.
    pub tree_filter: RwSignal<String>,
    /// Open tabs.
    pub tabs: RwSignal<Vec<Tab>>,
    /// Active tab identifier.
    pub active: RwSignal<u32>,
    next_tab: StoredValue<u32>,
    /// Query history, newest first.
    pub history: RwSignal<Vec<HistoryEntry>>,
    /// History filter.
    pub history_filter: RwSignal<String>,
    /// Preferences.
    pub prefs: RwSignal<Prefs>,
    /// Layout.
    pub layout: RwSignal<Layout>,
    /// Open overlay.
    pub overlay: RwSignal<Overlay>,
    /// Open context menu.
    pub menu: RwSignal<Option<Menu>>,
    /// Visible toasts.
    pub toasts: RwSignal<Vec<Toast>>,
    next_toast: StoredValue<u32>,
    /// Editor cursor.
    pub cursor: RwSignal<Cursor>,
    /// Whether the operating system prefers dark.
    pub system_dark: RwSignal<bool>,
    /// Clock for elapsed and relative times, ticking every second.
    pub clock: RwSignal<f64>,
    /// Cluster reachability.
    pub health: RwSignal<Health>,
    applied_schema: StoredValue<Option<String>>,
    pending_save: StoredValue<Option<String>>,
    save_timer: StoredValue<Option<TimeoutHandle>>,
}

impl App {
    /// Restores persisted state and opens the restored tabs in the editor.
    #[must_use]
    pub fn restore() -> Self {
        let saved = SavedState::restore(browser::storage_get(STATE_KEY).as_deref());
        let tabs: Vec<Tab> = saved.tabs.into_iter().map(Tab::new).collect();
        for tab in &tabs {
            host::open_model(&tab.key(), &tab.text.get_untracked());
        }
        let app = Self {
            config: RwSignal::new(None),
            config_error: RwSignal::new(None),
            databases: RwSignal::new(HashMap::new()),
            schemas: RwSignal::new(HashMap::new()),
            toggled: RwSignal::new(saved.toggled.into_iter().collect()),
            tree_filter: RwSignal::new(String::new()),
            tabs: RwSignal::new(tabs),
            active: RwSignal::new(saved.active_tab),
            next_tab: StoredValue::new(saved.next_tab.max(1)),
            history: RwSignal::new(restore_history(browser::storage_get(HISTORY_KEY).as_deref())),
            history_filter: RwSignal::new(String::new()),
            prefs: RwSignal::new(saved.prefs),
            layout: RwSignal::new(saved.layout),
            overlay: RwSignal::new(Overlay::None),
            menu: RwSignal::new(None),
            toasts: RwSignal::new(Vec::new()),
            next_toast: StoredValue::new(1),
            cursor: RwSignal::new(Cursor {
                line: 1,
                column: 1,
                selected: 0,
            }),
            system_dark: RwSignal::new(browser::prefers_dark()),
            clock: RwSignal::new(browser::now()),
            health: RwSignal::new(Health::Unknown),
            applied_schema: StoredValue::new(None),
            pending_save: StoredValue::new(None),
            save_timer: StoredValue::new(None),
        };
        if app.tabs.with_untracked(Vec::is_empty) {
            app.open_tab(NewTab {
                text: WELCOME_QUERY.to_owned(),
                ..NewTab::default()
            });
        } else {
            host::activate(&app.active.get_untracked().to_string());
        }
        app
    }

    /// Starts background work: configuration, editor events, persistence, theme, health, and the clock.
    pub fn start(self) {
        self.listen_to_editor();
        self.start_effects();
        let app = self;
        browser::on_prefers_dark_change(move |dark| app.system_dark.set(dark));
        let _ = set_interval_with_handle(move || app.clock.set(browser::now()), Duration::from_secs(1));
        let _ = set_interval_with_handle(move || app.check_health(), HEALTH_INTERVAL);
        window_event_listener_untyped("pagehide", move |_| app.flush_save());
        self.load_config();
    }

    fn listen_to_editor(self) {
        let app = self;
        let on_change =
            wasm_bindgen::closure::Closure::<dyn FnMut(String, String)>::new(move |id: String, text: String| {
                if let Some(tab) = app.tab(id.parse().unwrap_or_default()) {
                    tab.text.set(text);
                }
            });
        host::on_change(&on_change);
        on_change.forget();
        let on_cursor =
            wasm_bindgen::closure::Closure::<dyn FnMut(u32, u32, u32)>::new(move |line, column, selected| {
                app.cursor.set(Cursor { line, column, selected });
            });
        host::on_cursor(&on_cursor);
        on_cursor.forget();
        let on_command =
            wasm_bindgen::closure::Closure::<dyn FnMut(String)>::new(move |command: String| match command.as_str() {
                "run" => app.dispatch(Command::Run),
                "palette" => app.dispatch(Command::CommandPalette),
                _ => {}
            });
        host::on_command(&on_command);
        on_command.forget();
    }

    fn start_effects(self) {
        let app = self;
        Effect::new(move |_| {
            let dark = app.is_dark();
            browser::set_root_attribute("data-theme", if dark { "dark" } else { "light" });
            host::set_theme(dark);
        });
        Effect::new(move |_| {
            let size = app.prefs.with(|prefs| prefs.font_size);
            browser::set_root_property("--kx-editor-font-size", &format!("{size}px"));
            host::set_font_size(size);
        });
        Effect::new(move |_| {
            let state = SavedState {
                tabs: app.tabs.with(|tabs| tabs.iter().map(Tab::saved).collect()),
                active_tab: app.active.get(),
                next_tab: app.next_tab.get_value(),
                prefs: app.prefs.get(),
                layout: app.layout.get(),
                toggled: app.toggled.with(|toggled| toggled.iter().cloned().collect()),
            };
            app.schedule_save(serde_json::to_string(&state).unwrap_or_default());
        });
        Effect::new(move |_| {
            let json = app
                .history
                .with(|history| serde_json::to_string(history).unwrap_or_default());
            browser::storage_set(HISTORY_KEY, &json);
        });
        Effect::new(move |_| {
            let Some(tab) = app.active_tab() else { return };
            let (connection, database) = (tab.connection.get(), tab.database.get());
            if connection.is_empty() || database.is_empty() {
                return;
            }
            let key = db_key(&connection, &database);
            match app.schemas.with(|schemas| schemas.get(&key).cloned()) {
                None => app.load_schema(&connection, &database),
                Some(Load::Ready(schema))
                    if app
                        .applied_schema
                        .with_value(|applied| applied.as_deref() != Some(key.as_str())) =>
                {
                    app.applied_schema.set_value(Some(key));
                    let raw = serde_json::to_string(&schema.raw).unwrap_or_default();
                    spawn_local(async move {
                        if let Err(error) = host::set_schema(&raw, &schema.cluster_uri, &schema.database).await {
                            browser::console_warn(&format!(
                                "Could not load the schema into the editor: {}",
                                browser::js_message(&error)
                            ));
                        }
                    });
                }
                Some(_) => {}
            }
        });
    }

    fn schedule_save(self, json: String) {
        self.pending_save.set_value(Some(json));
        if let Some(handle) = self.save_timer.get_value() {
            handle.clear();
        }
        let app = self;
        self.save_timer
            .set_value(set_timeout_with_handle(move || app.flush_save(), SAVE_DELAY).ok());
    }

    fn flush_save(self) {
        if let Some(json) = self.pending_save.try_update_value(Option::take).flatten() {
            browser::storage_set(STATE_KEY, &json);
        }
    }

    /// Loads the server configuration and the databases of every connection.
    pub fn load_config(self) {
        let app = self;
        spawn_local(async move {
            match api::config().await {
                Ok(config) => {
                    app.config_error.set(None);
                    let connections = config.connections.clone();
                    app.config.set(Some(Arc::new(config)));
                    for connection in &connections {
                        app.load_databases(&connection.id);
                    }
                    app.repair_tabs();
                    app.open_shared_link();
                    app.check_health();
                }
                Err(error) => {
                    app.config_error.set(Some(error.message.clone()));
                    app.notify(
                        ToastKind::Error,
                        format!("Could not load the configuration: {}", error.message),
                    );
                }
            }
        });
    }

    fn check_health(self) {
        let app = self;
        spawn_local(async move {
            let health = match api::readiness().await {
                Ok(readiness) => Health::Checked(readiness),
                Err(error) => Health::ServerDown(error.message),
            };
            app.health.set(health);
        });
    }

    /// The configured connection, falling back to the first one.
    #[must_use]
    pub fn connection_info(&self, id: &str) -> Option<ConnectionInfo> {
        self.config.with_untracked(|config| {
            let connections = &config.as_ref()?.connections;
            connections
                .iter()
                .find(|connection| connection.id == id)
                .or_else(|| connections.first())
                .cloned()
        })
    }

    /// The default database of a connection once its databases are known.
    #[must_use]
    pub fn default_database(&self, connection: &ConnectionInfo) -> Option<String> {
        let databases = match self
            .databases
            .with_untracked(|databases| databases.get(&connection.id).cloned())
        {
            Some(Load::Ready(databases)) => databases,
            _ => return connection.default_database.clone(),
        };
        connection
            .default_database
            .clone()
            .filter(|name| databases.iter().any(|database| &database.name == name))
            .or_else(|| {
                databases
                    .iter()
                    .find(|database| database.name != "NetDefaultDB")
                    .map(|database| database.name.clone())
            })
            .or_else(|| databases.first().map(|database| database.name.clone()))
    }

    /// Points tabs at existing connections and fills in missing databases.
    fn repair_tabs(self) {
        for tab in self.tabs.get_untracked() {
            let Some(connection) = self.connection_info(&tab.connection.get_untracked()) else {
                continue;
            };
            if tab.connection.get_untracked() != connection.id {
                tab.connection.set(connection.id.clone());
                tab.database.set(String::new());
            }
            if tab.database.get_untracked().is_empty()
                && let Some(database) = self.default_database(&connection)
            {
                tab.database.set(database);
            }
        }
    }

    fn open_shared_link(self) {
        let Some(shared) = browser::take_fragment().and_then(|fragment| deeplink::decode(&fragment)) else {
            return;
        };
        let connection = self
            .connection_info(&shared.connection_id)
            .map(|connection| connection.id);
        self.open_tab(NewTab {
            title: Some("Shared query".to_owned()),
            text: shared.query,
            connection,
            database: Some(shared.database).filter(|database| !database.is_empty()),
            ..NewTab::default()
        });
        self.notify(ToastKind::Info, "Opened a shared query in a new tab.");
    }

    /// Loads the databases of a connection unless already loaded or loading.
    pub fn load_databases(self, connection: &str) {
        if self
            .databases
            .with_untracked(|databases| matches!(databases.get(connection), Some(Load::Loading | Load::Ready(_))))
        {
            return;
        }
        self.databases.update(|databases| {
            databases.insert(connection.to_owned(), Load::Loading);
        });
        let (app, connection) = (self, connection.to_owned());
        spawn_local(async move {
            let loaded = api::databases(&connection)
                .await
                .map(Arc::new)
                .map_err(|error| error.message);
            app.databases.update(|databases| {
                databases.insert(connection.clone(), loaded.map_or_else(Load::Failed, Load::Ready));
            });
            app.repair_tabs();
        });
    }

    /// Loads the schema of a database unless already loaded or loading.
    pub fn load_schema(self, connection: &str, database: &str) {
        let key = db_key(connection, database);
        if self
            .schemas
            .with_untracked(|schemas| matches!(schemas.get(&key), Some(Load::Loading | Load::Ready(_))))
        {
            return;
        }
        self.schemas.update(|schemas| {
            schemas.insert(key.clone(), Load::Loading);
        });
        let (app, connection, database) = (self, connection.to_owned(), database.to_owned());
        spawn_local(async move {
            let loaded = api::schema(&connection, &database)
                .await
                .map(Arc::new)
                .map_err(|error| error.message);
            app.schemas.update(|schemas| {
                schemas.insert(key, loaded.map_or_else(Load::Failed, Load::Ready));
            });
        });
    }

    /// Reloads databases and schemas.
    pub fn refresh(self) {
        self.applied_schema.set_value(None);
        self.schemas.set(HashMap::new());
        self.databases.set(HashMap::new());
        let connections = self.config.with_untracked(|config| {
            config
                .as_ref()
                .map(|config| config.connections.clone())
                .unwrap_or_default()
        });
        for connection in &connections {
            self.load_databases(&connection.id);
        }
        self.check_health();
        self.notify(ToastKind::Info, "Refreshing databases and schemas…");
    }

    /// Whether the effective theme is dark.
    #[must_use]
    pub fn is_dark(&self) -> bool {
        match self.prefs.with(|prefs| prefs.theme) {
            ThemeChoice::Dark => true,
            ThemeChoice::Light => false,
            ThemeChoice::System => self.system_dark.get(),
        }
    }

    /// The active tab, tracked.
    #[must_use]
    pub fn active_tab(&self) -> Option<Tab> {
        let active = self.active.get();
        self.tabs.with(|tabs| tabs.iter().find(|tab| tab.id == active).cloned())
    }

    /// The active tab, untracked.
    #[must_use]
    pub fn current_tab(&self) -> Option<Tab> {
        let active = self.active.get_untracked();
        self.tabs
            .with_untracked(|tabs| tabs.iter().find(|tab| tab.id == active).cloned())
    }

    /// A tab by identifier, untracked.
    #[must_use]
    pub fn tab(&self, id: u32) -> Option<Tab> {
        self.tabs
            .with_untracked(|tabs| tabs.iter().find(|tab| tab.id == id).cloned())
    }

    fn untitled(&self) -> String {
        let titles: BTreeSet<String> = self
            .tabs
            .with_untracked(|tabs| tabs.iter().map(|tab| tab.title.get_untracked()).collect());
        (1..)
            .map(|number| format!("Query {number}"))
            .find(|title| !titles.contains(title))
            .unwrap_or_default()
    }

    /// Opens a tab and makes it active.
    pub fn open_tab(self, options: NewTab) -> u32 {
        let id = self.next_tab.get_value();
        self.next_tab.set_value(id + 1);
        let current = self.current_tab();
        let connection = options
            .connection
            .or_else(|| current.as_ref().map(|tab| tab.connection.get_untracked()))
            .or_else(|| self.connection_info("").map(|connection| connection.id))
            .unwrap_or_default();
        let database = options
            .database
            .or_else(|| {
                current
                    .as_ref()
                    .filter(|tab| tab.connection.get_untracked() == connection)
                    .map(|tab| tab.database.get_untracked())
            })
            .filter(|database| !database.is_empty())
            .or_else(|| {
                self.connection_info(&connection)
                    .and_then(|connection| self.default_database(&connection))
            })
            .unwrap_or_default();
        let tab = Tab::new(SavedTab {
            id,
            title: options.title.unwrap_or_else(|| self.untitled()),
            text: options.text,
            connection_id: connection,
            database,
            file_name: options.file_name,
        });
        host::open_model(&tab.key(), &tab.text.get_untracked());
        let active = self.active.get_untracked();
        self.tabs.update(|tabs| {
            let at = tabs
                .iter()
                .position(|existing| existing.id == active)
                .map_or(tabs.len(), |index| index + 1);
            tabs.insert(at, tab.clone());
        });
        self.activate(id);
        if options.run {
            let query = tab.text.get_untracked();
            self.execute(&tab, query);
        }
        id
    }

    /// Makes a tab active and focuses the editor.
    pub fn activate(self, id: u32) {
        if self.tab(id).is_none() {
            return;
        }
        self.active.set(id);
        host::activate(&id.to_string());
        host::focus();
    }

    /// Closes a tab, keeping at least one open.
    pub fn close_tab(self, id: u32) {
        let Some(tab) = self.tab(id) else { return };
        if let Run::Running { request_id, .. } = tab.run.get_untracked() {
            api::abort(&request_id);
        }
        let index = self
            .tabs
            .with_untracked(|tabs| tabs.iter().position(|tab| tab.id == id))
            .unwrap_or(0);
        self.tabs.update(|tabs| tabs.retain(|tab| tab.id != id));
        host::close_model(&tab.key());
        if self.tabs.with_untracked(Vec::is_empty) {
            self.open_tab(NewTab::default());
        } else if self.active.get_untracked() == id {
            let next = self.tabs.with_untracked(|tabs| tabs[index.min(tabs.len() - 1)].id);
            self.activate(next);
        }
    }

    /// Closes every tab except `id`.
    pub fn close_others(self, id: u32) {
        for other in self.tabs.with_untracked(|tabs| {
            tabs.iter()
                .map(|tab| tab.id)
                .filter(|other| *other != id)
                .collect::<Vec<_>>()
        }) {
            self.close_tab(other);
        }
    }

    /// Opens a copy of a tab.
    pub fn duplicate_tab(self, id: u32) {
        let Some(tab) = self.tab(id) else { return };
        self.open_tab(NewTab {
            title: Some(format!("{} (copy)", tab.title.get_untracked())),
            text: host::get_text(&tab.key()),
            connection: Some(tab.connection.get_untracked()),
            database: Some(tab.database.get_untracked()),
            ..NewTab::default()
        });
    }

    fn cycle_tab(self, step: isize) {
        let active = self.active.get_untracked();
        let next = self.tabs.with_untracked(|tabs| {
            let index = tabs.iter().position(|tab| tab.id == active)? as isize;
            let count = tabs.len() as isize;
            tabs.get((index + step).rem_euclid(count) as usize).map(|tab| tab.id)
        });
        if let Some(next) = next {
            self.activate(next);
        }
    }

    /// Changes the database of the active tab.
    pub fn use_database(self, connection: &str, database: &str) {
        if let Some(tab) = self.current_tab() {
            tab.connection.set(connection.to_owned());
            tab.database.set(database.to_owned());
        }
    }

    /// Runs the selection, or the query block at the cursor, of the active tab.
    pub fn run(self) {
        let Some(tab) = self.current_tab() else { return };
        host::dismiss_suggestions();
        let selection = host::selection_text();
        let query = if selection.trim().is_empty() {
            blocks::block_at(&host::get_text(&tab.key()), host::cursor_line() as usize).unwrap_or_default()
        } else {
            selection
        };
        if strip_leading_trivia(&query).is_empty() {
            self.notify(
                ToastKind::Info,
                "Nothing to run: put the cursor inside a query or select some text.",
            );
            return;
        }
        self.execute(&tab, query);
    }

    /// Runs `query` in `tab`.
    pub fn execute(self, tab: &Tab, query: String) {
        if tab.run.with_untracked(Run::is_running) {
            self.notify(
                ToastKind::Info,
                "A query is already running in this tab. Cancel it with Shift+F5.",
            );
            return;
        }
        let (connection, database) = (tab.connection.get_untracked(), tab.database.get_untracked());
        if connection.is_empty() {
            self.notify(ToastKind::Error, "No cluster connection is configured.");
            return;
        }
        let command = is_management_command(&query);
        let request = QueryRequest {
            connection_id: connection.clone(),
            database: database.clone(),
            query: query.clone(),
            client_request_id: format!("kusto-explorer;{}", browser::uuid()),
            row_cap: Some(self.prefs.with_untracked(|prefs| prefs.row_cap)),
        };
        let started = browser::now();
        tab.run.set(Run::Running {
            request_id: request.client_request_id.clone(),
            connection: connection.clone(),
            started,
            command,
        });
        self.clock.set(started);
        let (app, tab) = (self, tab.clone());
        spawn_local(async move {
            let result = api::query(&request).await;
            let finished = browser::now();
            let elapsed = finished - started;
            let current = tab.run.with_untracked(
                |run| matches!(run, Run::Running { request_id, .. } if *request_id == request.client_request_id),
            );
            if !current {
                return;
            }
            let mut entry = HistoryEntry {
                id: request.client_request_id.clone(),
                at: started,
                connection_id: connection,
                database: database.clone(),
                query: query.clone(),
                kind: if command { QueryKind::Command } else { QueryKind::Query },
                duration_ms: elapsed,
                rows: 0,
                ok: true,
                error: None,
            };
            match result {
                Ok(response) => {
                    entry.rows = response.tables.iter().map(|table| table.total_rows).sum();
                    tab.reset_grid();
                    tab.chart_kind.set(None);
                    tab.pane.set(if response.visualization.is_some() {
                        Pane::Chart
                    } else {
                        Pane::Table(0)
                    });
                    tab.run.set(Run::Done(Arc::new(Outcome {
                        response,
                        query,
                        database,
                        elapsed,
                        finished,
                    })));
                }
                Err(error) if error.code.as_deref() == Some(api::ABORTED) => {
                    tab.run.set(Run::Cancelled { elapsed });
                    return;
                }
                Err(error) => {
                    entry.ok = false;
                    entry.error = Some(error.message.clone());
                    tab.run.set(Run::Failed(Arc::new(Failure { error, query, elapsed })));
                }
            }
            app.history.update(|history| push_history(history, entry));
        });
    }

    /// Cancels the running query of the active tab.
    pub fn cancel(self) {
        let Some(tab) = self.current_tab() else { return };
        let Run::Running {
            request_id,
            connection,
            started,
            ..
        } = tab.run.get_untracked()
        else {
            return;
        };
        api::abort(&request_id);
        tab.run.set(Run::Cancelled {
            elapsed: browser::now() - started,
        });
        spawn_local(async move {
            let _ = api::cancel(&CancelRequest {
                connection_id: connection,
                client_request_id: request_id,
            })
            .await;
        });
    }

    /// Shows a toast.
    pub fn notify(self, kind: ToastKind, text: impl Into<String>) {
        let id = self.next_toast.get_value();
        self.next_toast.set_value(id + 1);
        self.toasts.update(|toasts| {
            toasts.push(Toast {
                id,
                kind,
                text: text.into(),
            });
            if toasts.len() > 4 {
                toasts.remove(0);
            }
        });
        let app = self;
        set_timeout(move || app.dismiss(id), TOAST_LIFETIME);
    }

    /// Hides a toast.
    pub fn dismiss(self, id: u32) {
        self.toasts.update(|toasts| toasts.retain(|toast| toast.id != id));
    }

    /// Copies text and confirms with a toast.
    pub fn copy(self, text: String, what: &str) {
        let (app, what) = (self, what.to_owned());
        spawn_local(async move {
            if browser::copy_text(text).await {
                app.notify(ToastKind::Success, format!("Copied {what} to the clipboard."));
            } else {
                app.notify(ToastKind::Error, "The browser blocked clipboard access.");
            }
        });
    }

    /// Inserts text at the cursor of the active editor.
    pub fn insert(self, text: &str) {
        host::insert_text(text);
    }

    /// The finished outcome of the active tab with the table index the results view targets.
    #[must_use]
    pub fn current_table(&self) -> Option<(Arc<Outcome>, usize)> {
        let tab = self.current_tab()?;
        let Run::Done(outcome) = tab.run.get_untracked() else {
            return None;
        };
        let index = match tab.pane.get_untracked() {
            Pane::Table(index) => index,
            _ => outcome
                .response
                .visualization
                .as_ref()
                .map_or(0, |visualization| visualization.table_index),
        };
        (index < outcome.response.tables.len()).then_some((outcome, index))
    }

    fn export_text(&self, format: Format) -> Option<(String, String)> {
        let tab = self.current_tab()?;
        let (outcome, index) = self.current_table()?;
        let table: &ResultTable = &outcome.response.tables[index];
        let rows = format::visible_rows(table, &tab.filter.get_untracked(), tab.sort.get_untracked());
        let stem = sanitize_file_stem(&tab.title.get_untracked());
        let name = if outcome.response.tables.len() > 1 {
            format!("{stem}-{}", sanitize_file_stem(&table.name))
        } else {
            stem
        };
        Some((
            format!("{name}.{}", format.extension()),
            format::export(table, &rows, format),
        ))
    }

    /// Downloads the visible result rows.
    pub fn export(self, format: Format) {
        match self.export_text(format) {
            Some((name, text)) if browser::download(&name, format.mime(), &text) => {
                self.notify(ToastKind::Success, format!("Exported {name}."));
            }
            Some(_) => self.notify(ToastKind::Error, "The browser blocked the download."),
            None => self.notify(ToastKind::Info, "Run a query first: there are no results to export."),
        }
    }

    /// Copies the visible result rows.
    pub fn copy_results(self, format: Format) {
        match self.export_text(format) {
            Some((_, text)) => self.copy(text, &format!("the results as {}", format.label())),
            None => self.notify(ToastKind::Info, "Run a query first: there are no results to copy."),
        }
    }

    /// Copies a link that opens the active tab's query.
    pub fn share(self) {
        let Some(tab) = self.current_tab() else { return };
        let shared = SharedQuery {
            connection_id: tab.connection.get_untracked(),
            database: tab.database.get_untracked(),
            query: host::get_text(&tab.key()),
        };
        if strip_leading_trivia(&shared.query).is_empty() {
            self.notify(ToastKind::Info, "The query is empty: there is nothing to share.");
            return;
        }
        self.copy(
            browser::link_with_fragment(&deeplink::encode(&shared)),
            "a link to this query",
        );
    }

    /// Opens a query file in a new tab.
    pub fn open_file(self) {
        let app = self;
        browser::pick_text_file(".kql,.csl,.kusto,.txt", move |name, text| {
            app.open_tab(NewTab {
                title: Some(name.clone()),
                text,
                file_name: Some(name),
                ..NewTab::default()
            });
        });
    }

    /// Downloads the active tab as a query file.
    pub fn save_file(self) {
        let Some(tab) = self.current_tab() else { return };
        let name = tab
            .file_name
            .get_untracked()
            .unwrap_or_else(|| format!("{}.kql", sanitize_file_stem(&tab.title.get_untracked())));
        if browser::download(&name, "text/plain;charset=utf-8", &host::get_text(&tab.key())) {
            tab.title.set(name.clone());
            tab.file_name.set(Some(name.clone()));
            self.notify(ToastKind::Success, format!("Saved {name}."));
        }
    }

    /// Shows a side bar view; `toggle` hides it when it is already showing.
    pub fn show_view(self, view: SideView, toggle: bool) {
        self.layout.update(|layout| {
            if toggle && layout.sidebar_visible && layout.view == view {
                layout.sidebar_visible = false;
            } else {
                layout.view = view;
                layout.sidebar_visible = true;
            }
        });
    }

    fn zoom(self, size: Option<u32>, step: i32) {
        self.prefs.update(|prefs| {
            prefs.font_size = size
                .unwrap_or_else(|| prefs.font_size.saturating_add_signed(step))
                .clamp(MIN_FONT_SIZE, MAX_FONT_SIZE);
        });
    }

    /// Executes a command.
    pub fn dispatch(self, command: Command) {
        let active = self.active.get_untracked();
        match command {
            Command::Run => self.run(),
            Command::Cancel => self.cancel(),
            Command::NewTab => drop(self.open_tab(NewTab::default())),
            Command::CloseTab => self.close_tab(active),
            Command::DuplicateTab => self.duplicate_tab(active),
            Command::NextTab => self.cycle_tab(1),
            Command::PreviousTab => self.cycle_tab(-1),
            Command::OpenFile => self.open_file(),
            Command::SaveFile => self.save_file(),
            Command::Format => {
                host::focus();
                host::format_document();
            }
            Command::Share => self.share(),
            Command::CommandPalette => self.overlay.set(Overlay::Palette(">".to_owned())),
            Command::QuickOpen => self.overlay.set(Overlay::Palette(String::new())),
            Command::Shortcuts => self.overlay.set(Overlay::Shortcuts),
            Command::ToggleSidebar => self
                .layout
                .update(|layout| layout.sidebar_visible = !layout.sidebar_visible),
            Command::ShowConnections => self.show_view(SideView::Connections, false),
            Command::ShowHistory => self.show_view(SideView::History, false),
            Command::ShowStarters => self.show_view(SideView::Starters, false),
            Command::ShowSettings => self.show_view(SideView::Settings, false),
            Command::ToggleTheme => {
                let theme = if self.is_dark() {
                    ThemeChoice::Light
                } else {
                    ThemeChoice::Dark
                };
                self.prefs.update(|prefs| prefs.theme = theme);
            }
            Command::ThemeDark => self.prefs.update(|prefs| prefs.theme = ThemeChoice::Dark),
            Command::ThemeLight => self.prefs.update(|prefs| prefs.theme = ThemeChoice::Light),
            Command::ThemeSystem => self.prefs.update(|prefs| prefs.theme = ThemeChoice::System),
            Command::ZoomIn => self.zoom(None, 1),
            Command::ZoomOut => self.zoom(None, -1),
            Command::ZoomReset => self.zoom(Some(Prefs::default().font_size), 0),
            Command::RefreshSchema => self.refresh(),
            Command::ToggleResults => self
                .layout
                .update(|layout| layout.results_maximized = !layout.results_maximized),
            Command::ResetLayout => self.layout.update(|layout| {
                *layout = Layout {
                    view: layout.view,
                    ..Layout::default()
                }
            }),
            Command::FocusEditor => host::focus(),
            Command::ExportCsv => self.export(Format::Csv),
            Command::ExportJson => self.export(Format::Json),
            Command::ExportMarkdown => self.export(Format::Markdown),
            Command::CopyCsv => self.copy_results(Format::Csv),
            Command::CopyMarkdown => self.copy_results(Format::Markdown),
            Command::CopyDatatable => self.copy_results(Format::Datatable),
            Command::ClearHistory => {
                self.history.set(Vec::new());
                self.notify(ToastKind::Info, "Cleared the query history.");
            }
        }
    }
}

/// A file name stem made of safe characters.
#[must_use]
pub fn sanitize_file_stem(text: &str) -> String {
    let stem: String = text
        .trim()
        .trim_end_matches(".kql")
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let stem = stem.trim_matches(['-', '.']).to_lowercase();
    if stem.is_empty() {
        "query".to_owned()
    } else {
        stem.chars().take(80).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_stems_are_safe() {
        assert_eq!(sanitize_file_stem("Query 1"), "query-1");
        assert_eq!(sanitize_file_stem("report.kql"), "report");
        assert_eq!(sanitize_file_stem("../../etc/passwd"), "etc-passwd");
        assert_eq!(sanitize_file_stem("  "), "query");
    }

    #[test]
    fn database_keys_are_unambiguous() {
        assert_ne!(db_key("a", "bc"), db_key("ab", "c"));
    }
}
