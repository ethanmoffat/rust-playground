//! Results panel: result tables, the chart, statistics, and messages.

use std::{sync::Arc, time::Duration};

use kusto_explorer_api::MessageSeverity;
use leptos::{html, prelude::*};
use send_wrapper::SendWrapper;

use super::{chrome::rows_label, grid::Grid, icon, keys, open_menu_below};
use crate::{
    browser::{self, SizeObserver},
    chart::{self, ChartKind, Spec},
    format::Format,
    keymap::Command,
    state::{App, Failure, MenuEntry, Outcome, Pane, Run, Tab},
    time,
};

const FILTER_DELAY: Duration = Duration::from_millis(150);

#[component]
pub(crate) fn Results() -> impl IntoView {
    let app = expect_context::<App>();
    let tab = Memo::new(move |_| app.active_tab());
    view! {
        <section class="kx-results" data-testid="results" aria-label="Results">
            {move || tab.get().map(|tab| view! { <TabResults tab/> })}
        </section>
    }
}

fn table_label(name: &str, index: usize, count: usize) -> String {
    if count == 1 {
        "Results".to_owned()
    } else if name.is_empty() || name == "PrimaryResult" {
        format!("Table {}", index + 1)
    } else {
        name.to_owned()
    }
}

fn export_menu(app: App) -> Vec<MenuEntry> {
    vec![
        MenuEntry::item("Export as CSV", "export", move || app.export(Format::Csv)),
        MenuEntry::item("Export as JSON", "json", move || app.export(Format::Json)),
        MenuEntry::item("Export as Markdown", "markdown", move || app.export(Format::Markdown)),
        MenuEntry::Separator,
        MenuEntry::item("Copy as CSV", "copy", move || app.copy_results(Format::Csv)),
        MenuEntry::item("Copy as TSV (for spreadsheets)", "table", move || {
            app.copy_results(Format::Tsv)
        }),
        MenuEntry::item("Copy as Markdown table", "markdown", move || {
            app.copy_results(Format::Markdown)
        }),
        MenuEntry::item("Copy as KQL datatable", "code", move || {
            app.copy_results(Format::Datatable)
        }),
    ]
}

#[component]
fn TabResults(tab: Tab) -> impl IntoView {
    let app = expect_context::<App>();
    let maximized = Memo::new(move |_| app.layout.with(|layout| layout.results_maximized));
    let last_table = StoredValue::new(match tab.pane.get_untracked() {
        Pane::Table(index) => index,
        _ => 0,
    });
    let header_tabs = {
        let tab = tab.clone();
        move || {
            let Run::Done(outcome) = tab.run.get() else {
                return view! { <span class="kx-panel-title">"Results"</span> }.into_any();
            };
            let count = outcome.response.tables.len();
            let select = {
                let tab = tab.clone();
                move |pane: Pane| {
                    if let Pane::Table(index) = pane {
                        if last_table.get_value() != index {
                            tab.reset_grid();
                        }
                        last_table.set_value(index);
                    }
                    tab.pane.set(pane);
                }
            };
            let tables = outcome
                .response
                .tables
                .iter()
                .enumerate()
                .map(|(index, table)| {
                    let (pane, select) = (tab.pane.clone(), select.clone());
                    let label = table_label(&table.name, index, count);
                    let badge = time::format_count(table.total_rows as u64);
                    view! {
                        <button
                            class="kx-panel-tab"
                            class:active=move || pane.get() == Pane::Table(index)
                            role="tab"
                            data-testid="result-tab"
                            data-index=index.to_string()
                            title=format!("{} · {}", table.name, rows_label(table.total_rows))
                            on:click=move |_| select(Pane::Table(index))
                        >
                            <span>{label}</span>
                            <span class="kx-badge">{badge}</span>
                        </button>
                    }
                })
                .collect_view();
            let (chart_pane, stats_pane) = (tab.pane.clone(), tab.pane.clone());
            let (chart_select, stats_select) = (select.clone(), select);
            let messages = outcome.response.messages.len();
            view! {
                {tables}
                {(count > 0)
                    .then(|| {
                        view! {
                            <button
                                class="kx-panel-tab"
                                class:active=move || chart_pane.get() == Pane::Chart
                                role="tab"
                                data-testid="result-tab-chart"
                                on:click=move |_| chart_select(Pane::Chart)
                            >
                                {icon("graph")}
                                <span>"Chart"</span>
                            </button>
                        }
                    })}
                <button
                    class="kx-panel-tab"
                    class:active=move || stats_pane.get() == Pane::Stats
                    role="tab"
                    data-testid="result-tab-stats"
                    on:click=move |_| stats_select(Pane::Stats)
                >
                    {icon("pulse")}
                    <span>"Stats"</span>
                    {(messages > 0).then(|| view! { <span class="kx-badge">{messages}</span> })}
                </button>
            }
            .into_any()
        }
    };
    let showing_table = {
        let (run, pane) = (tab.run.clone(), tab.pane.clone());
        Memo::new(move |_| run.with(|run| matches!(run, Run::Done(_))) && matches!(pane.get(), Pane::Table(_)))
    };
    let has_results = {
        let run = tab.run.clone();
        Memo::new(move |_| run.with(|run| matches!(run, Run::Done(outcome) if !outcome.response.tables.is_empty())))
    };
    let body = {
        let tab = tab.clone();
        move || match tab.run.get() {
            Run::Idle => idle_view(),
            Run::Running { started, command, .. } => view! { <RunningView started command/> }.into_any(),
            Run::Failed(failure) => failure_view(app, &failure),
            Run::Cancelled { elapsed } => view! {
                <div class="kx-empty" data-testid="results-cancelled">
                    {icon("debug-stop")}
                    <p>{format!("Cancelled after {}.", time::format_duration(elapsed))}</p>
                </div>
            }
            .into_any(),
            Run::Done(outcome) => view! { <OutcomeView tab=tab.clone() outcome/> }.into_any(),
        }
    };
    view! {
        <div class="kx-panel-header">
            <div class="kx-panel-tabs" role="tablist" aria-label="Result views">{header_tabs}</div>
            <div class="kx-panel-actions">
                <Show when=move || showing_table.get()>
                    <GridFilter tab=tab.clone()/>
                </Show>
                <button
                    class="kx-icon-button"
                    data-testid="export-menu"
                    title="Export or copy results"
                    aria-label="Export or copy results"
                    disabled=move || !has_results.get()
                    on:click=move |event| open_menu_below(app, &event, export_menu(app))
                >
                    {icon("export")}
                </button>
                <button
                    class="kx-icon-button"
                    title="Maximize or restore results"
                    aria-label="Maximize or restore results"
                    on:click=move |_| app.dispatch(Command::ToggleResults)
                >
                    {move || icon(if maximized.get() { "screen-normal" } else { "screen-full" })}
                </button>
            </div>
        </div>
        <div class="kx-panel-body">{body}</div>
    }
}

#[component]
fn GridFilter(tab: Tab) -> impl IntoView {
    let timer = StoredValue::new(None::<TimeoutHandle>);
    let filter = tab.filter.clone();
    let shown = tab.filter.clone();
    view! {
        <label class="kx-search kx-grid-filter">
            {icon("search")}
            <input
                type="search"
                data-testid="grid-filter"
                placeholder="Search results"
                aria-label="Search results"
                prop:value=move || shown.get()
                on:input=move |event| {
                    let value = event_target_value(&event);
                    if let Some(handle) = timer.get_value() {
                        handle.clear();
                    }
                    let filter = filter.clone();
                    timer.set_value(set_timeout_with_handle(move || filter.set(value), FILTER_DELAY).ok());
                }
            />
        </label>
    }
}

fn idle_view() -> AnyView {
    view! {
        <div class="kx-empty kx-results-idle" data-testid="results-idle">
            {icon("play-circle")}
            <p>"Run a query to see its results here."</p>
            <dl class="kx-hints">
                <dt>"Run the query at the cursor"</dt>
                <dd>{keys("Shift+Enter")}</dd>
                <dt>"Run the query or the selection"</dt>
                <dd>{keys("F5")}</dd>
                <dt>"Show all commands"</dt>
                <dd>{keys("Ctrl+Shift+P")}</dd>
                <dt>"Go to a tab, table, or query"</dt>
                <dd>{keys("Ctrl+P")}</dd>
            </dl>
        </div>
    }
    .into_any()
}

#[component]
fn RunningView(started: f64, command: bool) -> impl IntoView {
    let app = expect_context::<App>();
    let now = RwSignal::new(browser::now());
    if let Ok(handle) = set_interval_with_handle(move || now.set(browser::now()), Duration::from_millis(100)) {
        on_cleanup(move || handle.clear());
    }
    let what = if command { "Running command" } else { "Running query" };
    view! {
        <div class="kx-running" data-testid="running">
            <div class="kx-progress" role="progressbar" aria-label=what>
                <div class="kx-progress-bar"></div>
            </div>
            <div class="kx-empty">
                {icon("loading codicon-modifier-spin")}
                <p>{move || format!("{what} · {}", time::format_duration((now.get() - started).max(0.0)))}</p>
                <button class="kx-button" on:click=move |_| app.cancel()>
                    {icon("debug-stop")}
                    <span>"Cancel"</span>
                    {keys("Shift+F5")}
                </button>
            </div>
        </div>
    }
}

fn failure_view(app: App, failure: &Arc<Failure>) -> AnyView {
    let error = failure.error.clone();
    let report = {
        let mut report = error.message.clone();
        if let Some(detail) = &error.detail {
            report = format!("{report}\n\n{detail}");
        }
        if let Some(code) = &error.code {
            report = format!("{report}\n\nCode: {code}");
        }
        format!("{report}\n\nQuery:\n{}", failure.query)
    };
    view! {
        <div class="kx-error" data-testid="query-error" role="alert">
            <div class="kx-error-header">
                {icon("error")}
                <span class="kx-error-title">"The query failed"</span>
                {error.code.clone().map(|code| view! { <span class="kx-badge">{code}</span> })}
                <span class="kx-spacer"></span>
                <span class="kx-muted">{format!("after {}", time::format_duration(failure.elapsed))}</span>
                <button class="kx-button" on:click=move |_| app.copy(report.clone(), "the error details")>
                    {icon("copy")}
                    <span>"Copy details"</span>
                </button>
            </div>
            <p class="kx-error-message">{error.message.clone()}</p>
            {error.detail.clone().filter(|detail| *detail != error.message).map(|detail| view! { <pre class="kx-error-detail">{detail}</pre> })}
        </div>
    }
    .into_any()
}

#[component]
fn OutcomeView(tab: Tab, outcome: Arc<Outcome>) -> impl IntoView {
    let pane = tab.pane.clone();
    move || {
        let count = outcome.response.tables.len();
        match pane.get() {
            Pane::Stats => view! { <StatsView outcome=Arc::clone(&outcome)/> }.into_any(),
            _ if count == 0 => view! {
                <div class="kx-empty" data-testid="results-empty">
                    {icon("info")}
                    <p>"The request completed without returning a table."</p>
                </div>
            }
            .into_any(),
            Pane::Chart => view! { <ChartView tab=tab.clone() outcome=Arc::clone(&outcome)/> }.into_any(),
            Pane::Table(index) => {
                view! { <Grid tab=tab.clone() outcome=Arc::clone(&outcome) index=index.min(count - 1)/> }.into_any()
            }
        }
    }
}

#[component]
fn ChartView(tab: Tab, outcome: Arc<Outcome>) -> impl IntoView {
    let table_index = outcome
        .response
        .visualization
        .as_ref()
        .map_or(0, |visualization| visualization.table_index)
        .min(outcome.response.tables.len() - 1);
    let chosen = tab.chart_kind.clone();
    let spec = {
        let outcome = Arc::clone(&outcome);
        Memo::new(move |_| {
            let kind = chosen.get();
            outcome
                .response
                .visualization
                .as_ref()
                .and_then(|visualization| Spec::from_visualization(visualization, kind))
                .unwrap_or_else(|| Spec::new(kind.unwrap_or(ChartKind::Column)))
        })
    };
    let model = Memo::new(move |_| spec.with(|spec| chart::build(&outcome.response.tables[table_index], spec)));
    let size = RwSignal::new((0.0_f64, 0.0_f64));
    let canvas = NodeRef::<html::Div>::new();
    let observer = StoredValue::new(None::<SendWrapper<SizeObserver>>);
    canvas.on_load(move |element| {
        let observed = browser::observe_size(&element, move |width, height| size.set((width.floor(), height.floor())));
        observer.set_value(observed.map(SendWrapper::new));
    });
    let svg = move || {
        let (width, height) = size.get();
        if width < 40.0 || height < 40.0 {
            return String::new();
        }
        model.with(|model| {
            model
                .as_ref()
                .map(|model| chart::render_svg(model, width, height))
                .unwrap_or_default()
        })
    };
    let kind = Memo::new(move |_| spec.with(|spec| spec.kind));
    let set_kind = tab.chart_kind.clone();
    view! {
        <div class="kx-chart" data-testid="chart">
            <div class="kx-chart-toolbar">
                <label class="kx-select" title="Chart type">
                    {move || icon(kind.get().icon())}
                    <select
                        data-testid="chart-kind"
                        aria-label="Chart type"
                        on:change=move |event| set_kind.set(ChartKind::from_id(&event_target_value(&event)))
                    >
                        {ChartKind::ALL
                            .into_iter()
                            .map(|option| {
                                view! { <option value=option.id() prop:selected=move || kind.get() == option>{option.label()}</option> }
                            })
                            .collect_view()}
                    </select>
                </label>
                <span class="kx-spacer"></span>
                {move || {
                    model
                        .with(|model| {
                            model.as_ref().ok().filter(|model| model.show_legend()).map(|model| {
                                model
                                    .legend()
                                    .into_iter()
                                    .map(|(name, color)| {
                                        let title = name.clone();
                                        view! {
                                            <span class="kx-legend-item" title=title>
                                                <span class=format!("kx-swatch kx-c{}", color % 10)></span>
                                                <span>{name}</span>
                                            </span>
                                        }
                                    })
                                    .collect_view()
                            })
                        })
                        .map(|items| view! { <div class="kx-legend" data-testid="chart-legend">{items}</div> })
                }}
            </div>
            {move || {
                model
                    .with(|model| match model {
                        Err(message) => Some(message.clone()),
                        Ok(_) => None,
                    })
                    .map(|message| {
                        view! {
                            <div class="kx-callout" data-testid="chart-error">
                                {icon("info")}
                                <span>{message}</span>
                            </div>
                        }
                    })
            }}
            <div class="kx-chart-canvas" node_ref=canvas inner_html=svg></div>
            {move || {
                model.with(|model| {
                    model.as_ref().ok().filter(|model| !model.notes.is_empty()).map(|model| {
                        view! {
                            <ul class="kx-chart-notes">
                                {model.notes.iter().map(|note| view! { <li>{note.clone()}</li> }).collect_view()}
                            </ul>
                        }
                    })
                })
            }}
        </div>
    }
}

#[component]
fn StatsView(outcome: Arc<Outcome>) -> impl IntoView {
    let stats = &outcome.response.stats;
    let mut cards: Vec<(&'static str, &'static str, String)> = vec![
        ("watch", "Round trip", time::format_duration(outcome.elapsed)),
        ("server", "Cluster time", time::format_duration(stats.server_elapsed_ms)),
    ];
    if let Some(execution) = stats.execution_time_ms {
        cards.push(("pulse", "Execution time", time::format_duration(execution)));
    }
    if let Some(cpu) = &stats.cpu_time {
        cards.push(("dashboard", "CPU time", cpu.clone()));
    }
    if let Some(memory) = stats.memory_peak_bytes {
        cards.push(("layers", "Peak memory", time::format_bytes(memory as f64)));
    }
    if let Some(rows) = stats.rows_scanned {
        cards.push(("list-flat", "Rows scanned", time::format_count(rows)));
    }
    if let Some(scanned) = stats.extents_scanned {
        let total = stats
            .extents_total
            .map(|total| format!(" of {}", time::format_count(total)))
            .unwrap_or_default();
        cards.push((
            "database",
            "Extents scanned",
            format!("{}{total}", time::format_count(scanned)),
        ));
    }
    cards.push(("table", "Result rows", time::format_count(stats.result_rows as u64)));
    cards.push((
        "cloud-upload",
        "Response size",
        time::format_bytes(stats.response_bytes as f64),
    ));
    let tables = outcome
        .response
        .tables
        .iter()
        .enumerate()
        .map(|(index, table)| {
            let truncated = table
                .truncated
                .then(|| format!(" (showing {})", time::format_count(table.rows.len() as u64)));
            view! {
                <tr>
                    <td>{index + 1}</td>
                    <td>{table.name.clone()}</td>
                    <td class="num">{table.columns.len()}</td>
                    <td class="num">{time::format_count(table.total_rows as u64)} {truncated}</td>
                </tr>
            }
        })
        .collect_view();
    let messages = outcome
        .response
        .messages
        .iter()
        .map(|message| {
            let (name, kind) = match message.severity {
                MessageSeverity::Info => ("info", "info"),
                MessageSeverity::Warning => ("warning", "warning"),
                MessageSeverity::Error => ("error", "error"),
            };
            view! {
                <li class="kx-message" data-kind=kind>
                    {icon(name)}
                    <span>{message.text.clone()}</span>
                </li>
            }
        })
        .collect_view();
    let has_messages = !outcome.response.messages.is_empty();
    view! {
        <div class="kx-stats" data-testid="stats">
            <div class="kx-cards">
                {cards
                    .into_iter()
                    .map(|(name, label, value)| {
                        view! {
                            <div class="kx-card">
                                <span class="kx-card-label">{icon(name)} {label}</span>
                                <span class="kx-card-value">{value}</span>
                            </div>
                        }
                    })
                    .collect_view()}
            </div>
            <h3>"Tables"</h3>
            <table class="kx-stats-table">
                <thead>
                    <tr>
                        <th>"#"</th>
                        <th>"Name"</th>
                        <th class="num">"Columns"</th>
                        <th class="num">"Rows"</th>
                    </tr>
                </thead>
                <tbody>{tables}</tbody>
            </table>
            {has_messages
                .then(|| {
                    view! {
                        <h3>"Messages"</h3>
                        <ul class="kx-messages" data-testid="messages">{messages}</ul>
                    }
                })}
            <h3>"Query"</h3>
            <p class="kx-muted">{format!("Database: {}", outcome.database)}</p>
            <pre class="kx-query-text">{outcome.query.clone()}</pre>
        </div>
    }
}
