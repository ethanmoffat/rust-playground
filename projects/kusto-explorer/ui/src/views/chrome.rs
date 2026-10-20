//! Title bar, activity bar, and status bar.

use kusto_explorer_api::ConnectionInfo;
use leptos::prelude::*;

use super::{LOGO_SVG, icon};
use crate::{
    keymap::Command,
    persist::{Prefs, SideView},
    state::{App, Pane, Run},
    time,
};

fn connection_by_id(app: App, id: &str) -> Option<ConnectionInfo> {
    app.config.with(|config| {
        config.as_ref().and_then(|config| {
            config
                .connections
                .iter()
                .find(|connection| connection.id == id)
                .cloned()
        })
    })
}

/// Row count with the right noun.
pub(crate) fn rows_label(rows: usize) -> String {
    let count = time::format_count(rows as u64);
    if rows == 1 {
        format!("{count} row")
    } else {
        format!("{count} rows")
    }
}

#[component]
pub(crate) fn TitleBar() -> impl IntoView {
    let app = expect_context::<App>();
    let label = move || {
        let tab = app.active_tab();
        let database = tab.as_ref().map(|tab| tab.database.get()).unwrap_or_default();
        let connection = tab.and_then(|tab| connection_by_id(app, &tab.connection.get()));
        match connection {
            Some(connection) if !database.is_empty() => format!("{database} — {}", connection.name),
            Some(connection) => connection.name,
            None => "Search tabs, tables, and commands".to_owned(),
        }
    };
    let sidebar_visible = Memo::new(move |_| app.layout.with(|layout| layout.sidebar_visible));
    let maximized = Memo::new(move |_| app.layout.with(|layout| layout.results_maximized));
    view! {
        <header class="kx-titlebar">
            <div class="kx-titlebar-brand">
                <span class="kx-logo" inner_html=LOGO_SVG></span>
                <span class="kx-brand-name">"kusto-explorer"</span>
            </div>
            <button
                class="kx-command-center"
                data-testid="command-center"
                title="Go to a tab, table, or query (Ctrl+P)"
                on:click=move |_| app.dispatch(Command::QuickOpen)
            >
                {icon("search")}
                <span class="kx-command-center-label">{label}</span>
            </button>
            <div class="kx-titlebar-actions">
                <button
                    class="kx-icon-button"
                    title="Toggle side bar (Ctrl+B)"
                    aria-label="Toggle side bar"
                    on:click=move |_| app.dispatch(Command::ToggleSidebar)
                >
                    {move || icon(if sidebar_visible.get() { "layout-sidebar-left" } else { "layout-sidebar-left-off" })}
                </button>
                <button
                    class="kx-icon-button"
                    title="Maximize or restore results"
                    aria-label="Maximize or restore results"
                    on:click=move |_| app.dispatch(Command::ToggleResults)
                >
                    {move || icon(if maximized.get() { "layout-panel-off" } else { "layout-panel" })}
                </button>
                <button
                    class="kx-icon-button"
                    data-testid="theme-toggle"
                    title=move || if app.is_dark() { "Switch to the light theme" } else { "Switch to the dark theme" }
                    aria-label="Toggle theme"
                    on:click=move |_| app.dispatch(Command::ToggleTheme)
                >
                    {icon("color-mode")}
                </button>
                <button
                    class="kx-icon-button"
                    title="Keyboard shortcuts (Shift+F1)"
                    aria-label="Keyboard shortcuts"
                    on:click=move |_| app.dispatch(Command::Shortcuts)
                >
                    {icon("keyboard")}
                </button>
            </div>
        </header>
    }
}

#[component]
pub(crate) fn ActivityBar() -> impl IntoView {
    let app = expect_context::<App>();
    let state = Memo::new(move |_| app.layout.with(|layout| (layout.sidebar_visible, layout.view)));
    let history_count = Memo::new(move |_| app.history.with(Vec::len));
    let item = move |view: SideView| {
        view! {
            <button
                class="kx-activity"
                class:active=move || state.get() == (true, view)
                data-testid=format!("activity-{}", view.id())
                title=view.title()
                aria-label=view.title()
                aria-pressed=move || (state.get() == (true, view)).to_string()
                on:click=move |_| app.show_view(view, true)
            >
                {icon(view.icon())}
                {(view == SideView::History)
                    .then(|| {
                        view! {
                            <span class="kx-activity-badge" class:hidden=move || history_count.get() == 0>
                                {move || history_count.get().min(99)}
                            </span>
                        }
                    })}
            </button>
        }
    };
    view! {
        <nav class="kx-activitybar" aria-label="Views">
            {item(SideView::Connections)}
            {item(SideView::History)}
            {item(SideView::Starters)}
            <span class="kx-spacer"></span>
            {item(SideView::Settings)}
        </nav>
    }
}

#[component]
pub(crate) fn StatusBar() -> impl IntoView {
    let app = expect_context::<App>();
    let tab = Memo::new(move |_| app.active_tab());
    let connection = Memo::new(move |_| tab.get().and_then(|tab| connection_by_id(app, &tab.connection.get())));
    let health = move || {
        let Some(connection) = connection.get() else {
            return ("unknown", "circle-outline", "No connection".to_owned());
        };
        match app.health.with(|health| health.connection(&connection.id)) {
            Some(true) => (
                "connected",
                "pass-filled",
                format!("Connected to {}", connection.cluster_uri),
            ),
            Some(false) => (
                "unreachable",
                "error",
                format!("{} is unreachable", connection.cluster_uri),
            ),
            None => (
                "unknown",
                "circle-outline",
                format!("Checking {}", connection.cluster_uri),
            ),
        }
    };
    let database = move || {
        tab.get()
            .map(|tab| tab.database.get())
            .filter(|database| !database.is_empty())
    };
    let run_status = move || {
        let tab = tab.get()?;
        let view = match tab.run.get() {
            Run::Idle => return None,
            Run::Running { started, .. } => view! {
                <span class="kx-status-item" data-testid="status-running">
                    {icon("loading codicon-modifier-spin")}
                    <span>{move || format!("Running {}", time::format_duration((app.clock.get() - started).max(0.0)))}</span>
                </span>
            }
            .into_any(),
            Run::Done(outcome) => {
                let index = match tab.pane.get() {
                    Pane::Table(index) => index,
                    _ => outcome.response.visualization.as_ref().map_or(0, |visualization| visualization.table_index),
                };
                let rows = outcome.response.tables.get(index).or_else(|| outcome.response.tables.first()).map_or(0, |table| table.total_rows);
                view! {
                    <span class="kx-status-item" data-testid="status-rows" title="Rows in the visible result table">
                        {icon("table")}
                        <span>{rows_label(rows)}</span>
                    </span>
                    <span class="kx-status-item" data-testid="status-duration" title="Round-trip time">
                        {icon("watch")}
                        <span>{time::format_duration(outcome.elapsed)}</span>
                    </span>
                }
                .into_any()
            }
            Run::Failed(_) => view! {
                <span class="kx-status-item kx-status-error" data-testid="status-failed">
                    {icon("error")}
                    <span>"Query failed"</span>
                </span>
            }
            .into_any(),
            Run::Cancelled { .. } => view! {
                <span class="kx-status-item" data-testid="status-cancelled">
                    {icon("debug-stop")}
                    <span>"Cancelled"</span>
                </span>
            }
            .into_any(),
        };
        Some(view)
    };
    let cursor = move || {
        let cursor = app.cursor.get();
        if cursor.selected > 0 {
            format!(
                "Ln {}, Col {} ({} selected)",
                cursor.line, cursor.column, cursor.selected
            )
        } else {
            format!("Ln {}, Col {}", cursor.line, cursor.column)
        }
    };
    let font_size = Memo::new(move |_| app.prefs.with(|prefs| prefs.font_size));
    view! {
        <footer class="kx-statusbar" data-testid="statusbar">
            <div class="kx-status-left">
                <button
                    class="kx-status-item kx-status-connection"
                    data-testid="status-connection"
                    data-state=move || health().0
                    title=move || health().2
                    on:click=move |_| app.show_view(SideView::Connections, false)
                >
                    {move || icon(health().1)}
                    <span>{move || connection.get().map_or_else(|| "No connection".to_owned(), |connection| connection.name)}</span>
                </button>
                {move || {
                    database()
                        .map(|database| {
                            view! {
                                <span class="kx-status-item" data-testid="status-database" title="Database of this tab">
                                    {icon("database")}
                                    <span>{database}</span>
                                </span>
                            }
                        })
                }}
            </div>
            <div class="kx-status-right">
                {run_status}
                <span class="kx-status-item" data-testid="status-cursor">{cursor}</span>
                <button
                    class="kx-status-item"
                    title="Editor font size; click to reset (Ctrl+0)"
                    on:click=move |_| app.prefs.update(|prefs| prefs.font_size = Prefs::default().font_size)
                >
                    {move || format!("{}px", font_size.get())}
                </button>
                <span class="kx-status-item" title="Kusto Query Language">"KQL"</span>
                <button class="kx-status-item" title="Show all commands (Ctrl+Shift+P)" on:click=move |_| app.dispatch(Command::CommandPalette)>
                    {icon("terminal")}
                </button>
            </div>
        </footer>
    }
}
