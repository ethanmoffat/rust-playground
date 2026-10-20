//! Editor group: query tabs, toolbar, the Monaco editor host, and the results split.

use leptos::{html, prelude::*};
use wasm_bindgen::JsCast;

use super::{icon, open_menu, results::Results};
use crate::{
    host,
    keymap::Command,
    state::{App, KEY_SEPARATOR, Load, MenuEntry, NewTab, Run, Tab, db_key},
};

const MIN_RESULTS_RATIO: f64 = 0.12;
const MAX_RESULTS_RATIO: f64 = 0.88;

#[component]
pub(crate) fn EditorGroup() -> impl IntoView {
    let app = expect_context::<App>();
    let split = NodeRef::<html::Div>::new();
    let editor = NodeRef::<html::Div>::new();
    editor.on_load(|element| {
        host::mount(&element);
        host::focus();
    });
    let layout = Memo::new(move |_| {
        app.layout
            .with(|layout| (layout.results_ratio, layout.results_maximized))
    });
    let rows = move || {
        let (ratio, maximized) = layout.get();
        if maximized {
            "grid-template-rows: 0 0 minmax(0, 1fr)".to_owned()
        } else {
            format!(
                "grid-template-rows: minmax(0, {:.4}fr) 0 minmax(0, {ratio:.4}fr)",
                1.0 - ratio
            )
        }
    };
    view! {
        <section class="kx-editor-group">
            <TabStrip/>
            <Toolbar/>
            <div class="kx-split" class:maximized=move || layout.get().1 node_ref=split style=rows>
                <div class="kx-editor-host" node_ref=editor data-testid="editor"></div>
                <SplitSash split/>
                <Results/>
            </div>
        </section>
    }
}

#[component]
fn SplitSash(split: NodeRef<html::Div>) -> impl IntoView {
    let app = expect_context::<App>();
    let dragging = RwSignal::new(false);
    view! {
        <div
            class="kx-sash kx-sash-horizontal"
            class:active=move || dragging.get()
            role="separator"
            aria-orientation="horizontal"
            title="Drag to resize; double-click to reset"
            on:pointerdown=move |event| {
                if event.button() != 0 {
                    return;
                }
                event.prevent_default();
                if let Some(target) = event.current_target().and_then(|target| target.dyn_into::<web_sys::Element>().ok()) {
                    let _ = target.set_pointer_capture(event.pointer_id());
                }
                dragging.set(true);
            }
            on:pointermove=move |event| {
                if !dragging.get_untracked() {
                    return;
                }
                let Some(container) = split.get_untracked() else { return };
                let rect = container.get_bounding_client_rect();
                if rect.height() <= 0.0 {
                    return;
                }
                let ratio = ((rect.bottom() - f64::from(event.client_y())) / rect.height()).clamp(MIN_RESULTS_RATIO, MAX_RESULTS_RATIO);
                app.layout.update(|layout| {
                    layout.results_ratio = ratio;
                    layout.results_maximized = false;
                });
            }
            on:pointerup=move |_| dragging.set(false)
            on:lostpointercapture=move |_| dragging.set(false)
            on:dblclick=move |_| app.layout.update(|layout| layout.results_ratio = crate::persist::Layout::default().results_ratio)
        ></div>
    }
}

#[component]
fn TabStrip() -> impl IntoView {
    let app = expect_context::<App>();
    let editing = RwSignal::new(None::<u32>);
    view! {
        <div class="kx-tabs" role="tablist" aria-label="Query tabs">
            <div class="kx-tabs-scroll">
                <For each=move || app.tabs.get() key=|tab| tab.id children=move |tab| view! { <TabItem tab editing/> }/>
            </div>
            <button
                class="kx-icon-button kx-tab-add"
                data-testid="new-tab"
                title="New query tab (Alt+N)"
                aria-label="New query tab"
                on:click=move |_| {
                    app.open_tab(NewTab::default());
                }
            >
                {icon("add")}
            </button>
        </div>
    }
}

fn tab_menu(app: App, id: u32) -> Vec<MenuEntry> {
    vec![
        MenuEntry::item("Close", "close", move || app.close_tab(id)),
        MenuEntry::item("Close others", "close-all", move || app.close_others(id)),
        MenuEntry::item("Duplicate", "copy", move || app.duplicate_tab(id)),
        MenuEntry::Separator,
        MenuEntry::item("Copy link to query", "link", move || {
            app.activate(id);
            app.share();
        }),
        MenuEntry::item("Save as .kql file", "save", move || {
            app.activate(id);
            app.save_file();
        }),
    ]
}

#[component]
fn TabItem(tab: Tab, editing: RwSignal<Option<u32>>) -> impl IntoView {
    let app = expect_context::<App>();
    let id = tab.id;
    let active = move || app.active.get() == id;
    let running = {
        let run = tab.run.clone();
        Memo::new(move |_| run.with(Run::is_running))
    };
    let title = tab.title.clone();
    let tooltip = {
        let (title, database, file_name) = (tab.title.clone(), tab.database.clone(), tab.file_name.clone());
        move || {
            let mut text = title.get();
            if let Some(file) = file_name.get().filter(|file| *file != text) {
                text = format!("{text} ({file})");
            }
            let database = database.get();
            if database.is_empty() {
                text
            } else {
                format!("{text} · {database}")
            }
        }
    };
    let name = move || {
        if editing.get() == Some(id) {
            view! { <RenameInput title=title.clone() editing/> }.into_any()
        } else {
            let title = title.clone();
            view! { <span class="kx-tab-title">{move || title.get()}</span> }.into_any()
        }
    };
    view! {
        <div
            class="kx-tab"
            class:active=active
            class:running=running
            role="tab"
            aria-selected=move || active().to_string()
            data-testid="editor-tab"
            title=tooltip
            on:click=move |_| app.activate(id)
            on:auxclick=move |event| {
                if event.button() == 1 {
                    event.prevent_default();
                    app.close_tab(id);
                }
            }
            on:dblclick=move |_| editing.set(Some(id))
            on:contextmenu=move |event| open_menu(app, &event, tab_menu(app, id))
        >
            {move || icon(if running.get() { "loading codicon-modifier-spin" } else { "file-code" })}
            {name}
            <button
                class="kx-tab-close"
                title="Close (Alt+W)"
                aria-label="Close tab"
                on:click=move |event| {
                    event.stop_propagation();
                    app.close_tab(id);
                }
            >
                {icon("close")}
            </button>
        </div>
    }
}

#[component]
fn RenameInput(title: ArcRwSignal<String>, editing: RwSignal<Option<u32>>) -> impl IntoView {
    let input = NodeRef::<html::Input>::new();
    input.on_load(|element| {
        let _ = element.focus();
        element.select();
    });
    let cancelled = StoredValue::new(false);
    let commit = {
        let title = title.clone();
        move |value: String| {
            if cancelled.get_value() {
                return;
            }
            let value = value.trim().to_owned();
            if !value.is_empty() {
                title.set(value);
            }
            editing.set(None);
        }
    };
    let on_key = {
        let commit = commit.clone();
        move |event: web_sys::KeyboardEvent| match event.key().as_str() {
            "Enter" => {
                event.prevent_default();
                commit(event_target_value(&event));
                host::focus();
            }
            "Escape" => {
                event.prevent_default();
                event.stop_propagation();
                cancelled.set_value(true);
                editing.set(None);
                host::focus();
            }
            _ => {}
        }
    };
    view! {
        <input
            class="kx-tab-rename"
            node_ref=input
            aria-label="Tab name"
            value=title.get_untracked()
            on:click=|event| event.stop_propagation()
            on:dblclick=|event| event.stop_propagation()
            on:keydown=on_key
            on:blur=move |event| commit(event_target_value(&event))
        />
    }
}

#[component]
fn Toolbar() -> impl IntoView {
    let app = expect_context::<App>();
    let running = Memo::new(move |_| app.active_tab().is_some_and(|tab| tab.run.with(Run::is_running)));
    view! {
        <div class="kx-toolbar" role="toolbar" aria-label="Query">
            <button
                class="kx-button kx-button-primary"
                data-testid="run"
                title="Run the query at the cursor or the selection (F5 or Shift+Enter)"
                disabled=move || running.get()
                on:click=move |_| app.run()
            >
                {icon("play")}
                <span>"Run"</span>
            </button>
            <button
                class="kx-button"
                data-testid="cancel"
                title="Cancel the running query (Shift+F5)"
                disabled=move || !running.get()
                on:click=move |_| app.cancel()
            >
                {icon("debug-stop")}
                <span>"Cancel"</span>
            </button>
            <span class="kx-toolbar-separator"></span>
            <DatabasePicker/>
            <span class="kx-spacer"></span>
            <button class="kx-icon-button" title="Format query (Shift+Alt+F)" aria-label="Format query" on:click=move |_| app.dispatch(Command::Format)>
                {icon("list-flat")}
            </button>
            <button class="kx-icon-button" title="Open a .kql file (Ctrl+O)" aria-label="Open file" on:click=move |_| app.open_file()>
                {icon("folder-opened")}
            </button>
            <button class="kx-icon-button" title="Save as a .kql file (Ctrl+S)" aria-label="Save file" on:click=move |_| app.save_file()>
                {icon("save")}
            </button>
            <button class="kx-icon-button" data-testid="share" title="Copy a link to this query" aria-label="Copy link" on:click=move |_| app.share()>
                {icon("link")}
            </button>
        </div>
    }
}

#[derive(Clone, Debug, PartialEq)]
struct DatabaseGroup {
    connection_id: String,
    connection_name: String,
    databases: Vec<String>,
    status: Option<String>,
}

#[component]
fn DatabasePicker() -> impl IntoView {
    let app = expect_context::<App>();
    let current = Memo::new(move |_| {
        app.active_tab()
            .map(|tab| (tab.connection.get(), tab.database.get()))
            .unwrap_or_default()
    });
    let groups = Memo::new(move |_| {
        let connections = app.config.with(|config| {
            config
                .as_ref()
                .map(|config| config.connections.clone())
                .unwrap_or_default()
        });
        app.databases.with(|databases| {
            connections
                .into_iter()
                .map(|connection| {
                    let (names, status) = match databases.get(&connection.id) {
                        Some(Load::Ready(list)) => (list.iter().map(|database| database.name.clone()).collect(), None),
                        Some(Load::Failed(error)) => (Vec::new(), Some(format!("unavailable: {error}"))),
                        _ => (Vec::new(), Some("loading…".to_owned())),
                    };
                    DatabaseGroup {
                        connection_id: connection.id,
                        connection_name: connection.name,
                        databases: names,
                        status,
                    }
                })
                .collect::<Vec<_>>()
        })
    });
    let options = move || {
        let (connection, database) = current.get();
        let groups = groups.get();
        let known = groups
            .iter()
            .any(|group| group.connection_id == connection && group.databases.contains(&database));
        let orphan = (!known && !database.is_empty()).then(|| {
            let key = db_key(&connection, &database);
            view! { <option value=key selected=true>{database.clone()}</option> }
        });
        let empty = (!known && database.is_empty())
            .then(|| view! { <option value="" selected=true disabled=true>"Select a database"</option> });
        let groups = groups
            .into_iter()
            .map(|group| {
                let label = match &group.status {
                    Some(status) => format!("{} ({status})", group.connection_name),
                    None => group.connection_name.clone(),
                };
                let items = group
                    .databases
                    .iter()
                    .map(|name| {
                        let selected = group.connection_id == connection && *name == database;
                        view! { <option value=db_key(&group.connection_id, name) selected=selected>{name.clone()}</option> }
                    })
                    .collect_view();
                view! { <optgroup label=label>{items}</optgroup> }
            })
            .collect_view();
        view! {
            {empty}
            {orphan}
            {groups}
        }
    };
    view! {
        <label class="kx-select" title="Database for this tab">
            {icon("database")}
            <select
                data-testid="database-select"
                aria-label="Database"
                on:change=move |event| {
                    let value = event_target_value(&event);
                    if let Some((connection, database)) = value.split_once(KEY_SEPARATOR) {
                        app.use_database(connection, database);
                    }
                    host::focus();
                }
            >
                {options}
            </select>
        </label>
    }
}
