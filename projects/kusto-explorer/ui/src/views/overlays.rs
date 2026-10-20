//! Overlays: the command palette and quick open, the shortcut sheet, context menus, and toasts.

use leptos::{html, prelude::*};

use super::{
    icon, keys,
    sidebar::{EntityKind, query_tab},
};
use crate::{
    blocks, browser,
    format::kql_name,
    fuzzy::fuzzy_match,
    host,
    keymap::{ALTERNATE_KEYS, COMMANDS, Command},
    starters::STARTERS,
    state::{App, Load, MenuEntry, NewTab, Overlay, ToastKind},
    time,
};

const PALETTE_LIMIT: usize = 50;
const RECENT_HISTORY: usize = 8;
const MENU_WIDTH: f64 = 248.0;
const MENU_ITEM_HEIGHT: f64 = 26.0;
const MENU_SEPARATOR_HEIGHT: f64 = 9.0;
const MENU_MARGIN: f64 = 4.0;

/// Editor shortcuts provided by the code editor itself.
const EDITOR_KEYS: &[(&str, &str)] = &[
    ("Trigger suggestions", "Ctrl+Space"),
    ("Toggle line comment", "Ctrl+/"),
    ("Find", "Ctrl+F"),
    ("Replace", "Ctrl+H"),
    ("Select next match", "Ctrl+D"),
    ("Move line up or down", "Alt+Up"),
    ("Copy line down", "Shift+Alt+Down"),
];

#[component]
pub(crate) fn Overlays() -> impl IntoView {
    let app = expect_context::<App>();
    view! {
        {move || match app.overlay.get() {
            Overlay::None => None,
            Overlay::Palette(initial) => Some(view! { <Palette initial/> }.into_any()),
            Overlay::Shortcuts => Some(view! { <ShortcutSheet/> }.into_any()),
        }}
        <ContextMenu/>
        <Toasts/>
    }
}

fn close_overlay(app: App) {
    app.overlay.set(Overlay::None);
    host::focus();
}

/// What a palette entry does.
#[derive(Clone, Debug, PartialEq)]
enum Target {
    Command(Command),
    Tab(u32),
    Entity {
        connection: String,
        database: String,
        kind: EntityKind,
        name: String,
    },
    Starter(&'static str),
    History(String),
    Database {
        connection: String,
        database: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
struct PaletteItem {
    icon: &'static str,
    label: String,
    detail: String,
    keys: &'static str,
    positions: Vec<usize>,
    score: i32,
    target: Target,
}

impl PaletteItem {
    fn matched(query: &str, icon: &'static str, label: String, detail: String, target: Target) -> Option<Self> {
        let found = fuzzy_match(query, &label)?;
        Some(Self {
            icon,
            label,
            detail,
            keys: "",
            positions: found.positions,
            score: found.score,
            target,
        })
    }
}

fn command_items(query: &str) -> Vec<PaletteItem> {
    COMMANDS
        .iter()
        .filter_map(|info| {
            let item = PaletteItem::matched(
                query,
                info.icon,
                format!("{}: {}", info.category, info.title),
                String::new(),
                Target::Command(info.command),
            )?;
            Some(PaletteItem {
                keys: info.keys,
                ..item
            })
        })
        .collect()
}

fn entity_label(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Table => "Table",
        EntityKind::MaterializedView => "Materialized view",
        EntityKind::ExternalTable => "External table",
        EntityKind::Function => "Function",
    }
}

fn quick_open_items(app: App, query: &str) -> Vec<PaletteItem> {
    let mut items = Vec::new();
    app.tabs.with(|tabs| {
        for tab in tabs {
            let title = tab.title.get();
            let detail = format!("Open tab · {}", tab.database.get());
            items.extend(PaletteItem::matched(
                query,
                "file-code",
                if title.is_empty() { "Untitled".to_owned() } else { title },
                detail,
                Target::Tab(tab.id),
            ));
        }
    });
    if !query.is_empty() {
        app.schemas.with(|schemas| {
            for schema in schemas.values().filter_map(|load| match load {
                Load::Ready(schema) => Some(schema),
                Load::Loading | Load::Failed(_) => None,
            }) {
                for kind in EntityKind::ALL {
                    let names: Vec<&str> = match kind {
                        EntityKind::Table => schema.tables.iter().map(|entity| entity.name.as_str()).collect(),
                        EntityKind::MaterializedView => schema
                            .materialized_views
                            .iter()
                            .map(|entity| entity.name.as_str())
                            .collect(),
                        EntityKind::ExternalTable => schema
                            .external_tables
                            .iter()
                            .map(|entity| entity.name.as_str())
                            .collect(),
                        EntityKind::Function => {
                            schema.functions.iter().map(|function| function.name.as_str()).collect()
                        }
                    };
                    let action = if kind == EntityKind::Function {
                        "show definition"
                    } else {
                        "preview 100 rows"
                    };
                    for name in names {
                        let target = Target::Entity {
                            connection: schema.connection_id.clone(),
                            database: schema.database.clone(),
                            kind,
                            name: name.to_owned(),
                        };
                        let detail = format!("{} in {} · {action}", entity_label(kind), schema.database);
                        items.extend(PaletteItem::matched(
                            query,
                            kind.icon(),
                            name.to_owned(),
                            detail,
                            target,
                        ));
                    }
                }
            }
        });
    }
    for starter in STARTERS {
        let detail = format!("Starter · {}", starter.description);
        items.extend(PaletteItem::matched(
            query,
            starter.icon,
            starter.title.to_owned(),
            detail,
            Target::Starter(starter.id),
        ));
    }
    let now = app.clock.get_untracked();
    app.history.with(|history| {
        let limit = if query.is_empty() {
            RECENT_HISTORY
        } else {
            history.len()
        };
        for entry in history.iter().take(limit) {
            let detail = format!("History · {} · {}", entry.database, time::format_ago(entry.at, now));
            items.extend(PaletteItem::matched(
                query,
                "history",
                blocks::summary(&entry.query, 90),
                detail,
                Target::History(entry.id.clone()),
            ));
        }
    });
    if !query.is_empty() {
        let names = app.config.with(|config| {
            config.as_ref().map(|config| {
                config
                    .connections
                    .iter()
                    .map(|connection| (connection.id.clone(), connection.name.clone()))
                    .collect::<Vec<_>>()
            })
        });
        app.databases.with(|databases| {
            for (connection, name) in names.unwrap_or_default() {
                let Some(Load::Ready(list)) = databases.get(&connection) else {
                    continue;
                };
                for database in list.iter() {
                    let target = Target::Database {
                        connection: connection.clone(),
                        database: database.name.clone(),
                    };
                    let detail = format!("Database on {name} · use in the current tab");
                    items.extend(PaletteItem::matched(
                        query,
                        "database",
                        database.name.clone(),
                        detail,
                        target,
                    ));
                }
            }
        });
    }
    items
}

fn palette_items(app: App, query: &str) -> Vec<PaletteItem> {
    let mut items = match query.strip_prefix('>') {
        Some(rest) => command_items(rest.trim()),
        None => quick_open_items(app, query.trim()),
    };
    // A stable sort keeps the group order for equal scores.
    items.sort_by_key(|item| std::cmp::Reverse(item.score));
    items.truncate(PALETTE_LIMIT);
    items
}

fn accept(app: App, target: Target) {
    close_overlay(app);
    match target {
        Target::Command(command) => app.dispatch(command),
        Target::Tab(id) => app.activate(id),
        Target::Entity {
            connection,
            database,
            kind,
            name,
        } => {
            let query = if kind == EntityKind::Function {
                format!(".show function {}", kql_name(&name))
            } else {
                format!("{}\n| take 100", kind.reference(&name))
            };
            query_tab(app, name, query, &connection, &database, true);
        }
        Target::Starter(id) => {
            if let Some(starter) = STARTERS.iter().find(|starter| starter.id == id) {
                app.open_tab(NewTab {
                    title: Some(starter.title.to_owned()),
                    text: starter.query.to_owned(),
                    run: true,
                    ..NewTab::default()
                });
            }
        }
        Target::History(id) => {
            if let Some(entry) = app
                .history
                .with_untracked(|history| history.iter().find(|entry| entry.id == id).cloned())
            {
                query_tab(
                    app,
                    String::new(),
                    entry.query,
                    &entry.connection_id,
                    &entry.database,
                    false,
                );
            }
        }
        Target::Database { connection, database } => app.use_database(&connection, &database),
    }
}

fn highlight(label: &str, positions: &[usize]) -> AnyView {
    let mut segments: Vec<(bool, String)> = Vec::new();
    for (index, character) in label.chars().enumerate() {
        let marked = positions.contains(&index);
        match segments.last_mut() {
            Some((last, text)) if *last == marked => text.push(character),
            _ => segments.push((marked, character.to_string())),
        }
    }
    segments
        .into_iter()
        .map(|(marked, text)| {
            if marked {
                view! { <mark>{text}</mark> }.into_any()
            } else {
                text.into_any()
            }
        })
        .collect_view()
        .into_any()
}

#[component]
fn Palette(initial: String) -> impl IntoView {
    let app = expect_context::<App>();
    let commands = initial.starts_with('>');
    let query = RwSignal::new(initial);
    let selected = RwSignal::new(0_usize);
    let items = Memo::new(move |_| query.with(|query| palette_items(app, query)));
    let input = NodeRef::<html::Input>::new();
    input.on_load(|element| {
        let _ = element.focus();
        let end = element.value().encode_utf16().count() as u32;
        let _ = element.set_selection_range(end, end);
    });
    Effect::new(move |_| {
        selected.track();
        request_animation_frame(|| browser::scroll_into_view(".kx-palette-item.selected"));
    });
    let on_key = move |event: web_sys::KeyboardEvent| {
        let count = items.with(Vec::len);
        match event.key().as_str() {
            "ArrowDown" if count > 0 => {
                event.prevent_default();
                selected.update(|selected| *selected = (*selected + 1) % count);
            }
            "ArrowUp" if count > 0 => {
                event.prevent_default();
                selected.update(|selected| *selected = (*selected + count - 1) % count);
            }
            "Enter" => {
                event.prevent_default();
                let target = items.with(|items| items.get(selected.get_untracked()).map(|item| item.target.clone()));
                if let Some(target) = target {
                    accept(app, target);
                }
            }
            _ => {}
        }
    };
    let list = move || {
        let items = items.get();
        if items.is_empty() {
            return view! { <div class="kx-palette-empty">"No matching results"</div> }.into_any();
        }
        items
            .into_iter()
            .enumerate()
            .map(|(index, item)| {
                let PaletteItem {
                    icon: name,
                    label,
                    detail,
                    keys: shortcut,
                    positions,
                    target,
                    ..
                } = item;
                view! {
                    <div
                        class="kx-palette-item"
                        class:selected=move || selected.get() == index
                        role="option"
                        aria-selected=move || (selected.get() == index).to_string()
                        data-testid="palette-item"
                        on:mousemove=move |_| {
                            if selected.get_untracked() != index {
                                selected.set(index);
                            }
                        }
                        on:click=move |_| accept(app, target.clone())
                    >
                        {icon(name)}
                        <span class="kx-palette-label">{highlight(&label, &positions)}</span>
                        <span class="kx-palette-detail">{detail}</span>
                        {(!shortcut.is_empty()).then(|| keys(shortcut))}
                    </div>
                }
            })
            .collect_view()
            .into_any()
    };
    let placeholder = if commands {
        "Type the name of a command"
    } else {
        "Go to a tab, table, starter, or recent query (type > for commands)"
    };
    view! {
        <div class="kx-overlay-backdrop kx-palette-backdrop" on:click=move |_| close_overlay(app)>
            <div class="kx-palette" data-testid="palette" role="dialog" aria-label="Command palette" on:click=|event| event.stop_propagation()>
                <input
                    class="kx-palette-input"
                    node_ref=input
                    data-testid="palette-input"
                    type="text"
                    spellcheck="false"
                    autocomplete="off"
                    placeholder=placeholder
                    aria-label=placeholder
                    prop:value=move || query.get()
                    on:input=move |event| {
                        query.set(event_target_value(&event));
                        selected.set(0);
                    }
                    on:keydown=on_key
                />
                <div class="kx-palette-list" role="listbox">{list}</div>
                <div class="kx-palette-footer">
                    <span>{keys("↑")} {keys("↓")} " to move"</span>
                    <span>{keys("Enter")} " to select"</span>
                    <span>{keys("Esc")} " to close"</span>
                </div>
            </div>
        </div>
    }
}

/// A category with its commands and their key bindings.
type ShortcutGroup = (&'static str, Vec<(&'static str, Vec<&'static str>)>);

#[component]
fn ShortcutSheet() -> impl IntoView {
    let app = expect_context::<App>();
    let mut groups: Vec<ShortcutGroup> = Vec::new();
    for info in COMMANDS.iter().filter(|info| !info.keys.is_empty()) {
        let mut bindings = vec![info.keys];
        bindings.extend(
            ALTERNATE_KEYS
                .iter()
                .filter(|(title, _)| *title == info.title)
                .map(|(_, keys)| *keys),
        );
        match groups.iter_mut().find(|(category, _)| *category == info.category) {
            Some((_, entries)) => entries.push((info.title, bindings)),
            None => groups.push((info.category, vec![(info.title, bindings)])),
        }
    }
    groups.push((
        "Editor",
        EDITOR_KEYS
            .iter()
            .map(|(title, shortcut)| (*title, vec![*shortcut]))
            .collect(),
    ));
    let sections = groups
        .into_iter()
        .map(|(category, entries)| {
            view! {
                <section class="kx-shortcut-group">
                    <h3>{category}</h3>
                    <dl>
                        {entries
                            .into_iter()
                            .map(|(title, bindings)| {
                                view! {
                                    <dt>{title}</dt>
                                    <dd>{bindings.into_iter().map(keys).collect_view()}</dd>
                                }
                            })
                            .collect_view()}
                    </dl>
                </section>
            }
        })
        .collect_view();
    view! {
        <div class="kx-overlay-backdrop" on:click=move |_| close_overlay(app)>
            <div
                class="kx-dialog kx-shortcuts"
                data-testid="shortcuts"
                role="dialog"
                aria-label="Keyboard shortcuts"
                on:click=|event| event.stop_propagation()
            >
                <div class="kx-dialog-header">
                    {icon("keyboard")}
                    <h2>"Keyboard shortcuts"</h2>
                    <span class="kx-spacer"></span>
                    <button class="kx-icon-button" title="Close" aria-label="Close" on:click=move |_| close_overlay(app)>
                        {icon("close")}
                    </button>
                </div>
                <div class="kx-dialog-body kx-shortcut-grid">{sections}</div>
                <p class="kx-dialog-footer kx-muted">"On macOS, use Cmd in place of Ctrl."</p>
            </div>
        </div>
    }
}

fn menu_position(x: f64, y: f64, entries: &[MenuEntry], viewport: (f64, f64)) -> (f64, f64) {
    let height = entries
        .iter()
        .map(|entry| match entry {
            MenuEntry::Item { .. } => MENU_ITEM_HEIGHT,
            MenuEntry::Separator => MENU_SEPARATOR_HEIGHT,
        })
        .sum::<f64>()
        + 2.0 * MENU_MARGIN;
    let left = x.min(viewport.0 - MENU_WIDTH - MENU_MARGIN).max(MENU_MARGIN);
    let top = y.min(viewport.1 - height - MENU_MARGIN).max(MENU_MARGIN);
    (left, top)
}

#[component]
fn ContextMenu() -> impl IntoView {
    let app = expect_context::<App>();
    move || {
        app.menu.get().map(|menu| {
            let (left, top) = menu_position(menu.x, menu.y, &menu.entries, browser::viewport_size());
            let entries = menu
                .entries
                .into_iter()
                .map(|entry| match entry {
                    MenuEntry::Item { label, icon: name, action } => view! {
                        <button
                            class="kx-menu-item"
                            role="menuitem"
                            data-testid="menu-item"
                            on:click=move |_| {
                                app.menu.set(None);
                                action();
                            }
                        >
                            {icon(name)}
                            <span>{label}</span>
                        </button>
                    }
                    .into_any(),
                    MenuEntry::Separator => view! { <div class="kx-menu-separator" role="separator"></div> }.into_any(),
                })
                .collect_view();
            view! {
                <div
                    class="kx-menu-backdrop"
                    on:click=move |_| app.menu.set(None)
                    on:contextmenu=move |event| {
                        event.prevent_default();
                        app.menu.set(None);
                    }
                ></div>
                <div class="kx-menu" role="menu" data-testid="context-menu" style=format!("left: {left}px; top: {top}px; width: {MENU_WIDTH}px")>
                    {entries}
                </div>
            }
        })
    }
}

#[component]
fn Toasts() -> impl IntoView {
    let app = expect_context::<App>();
    view! {
        <div class="kx-toasts" aria-live="polite">
            <For
                each=move || app.toasts.get()
                key=|toast| toast.id
                children=move |toast| {
                    let (name, kind) = match toast.kind {
                        ToastKind::Info => ("info", "info"),
                        ToastKind::Success => ("pass-filled", "success"),
                        ToastKind::Error => ("error", "error"),
                    };
                    let id = toast.id;
                    view! {
                        <div class="kx-toast" data-testid="toast" data-kind=kind role="status">
                            {icon(name)}
                            <span class="kx-toast-text">{toast.text}</span>
                            <button class="kx-icon-button" title="Dismiss" aria-label="Dismiss" on:click=move |_| app.dismiss(id)>
                                {icon("close")}
                            </button>
                        </div>
                    }
                }
            />
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_mode_lists_matching_commands_with_shortcuts() {
        let items = command_items("run");
        assert_eq!(
            items.first().map(|item| item.target.clone()),
            Some(Target::Command(Command::Run))
        );
        assert_eq!(items.first().map(|item| item.keys), Some("F5"));
        assert_eq!(command_items("").len(), COMMANDS.len());
        assert!(command_items("zzzz").is_empty());
    }

    #[test]
    fn menus_stay_inside_the_viewport() {
        let entries = vec![
            MenuEntry::item("A", "copy", || {}),
            MenuEntry::Separator,
            MenuEntry::item("B", "copy", || {}),
        ];
        assert_eq!(menu_position(10.0, 20.0, &entries, (1_000.0, 800.0)), (10.0, 20.0));
        let (left, top) = menu_position(990.0, 790.0, &entries, (1_000.0, 800.0));
        assert!((left - (1_000.0 - MENU_WIDTH - MENU_MARGIN)).abs() < f64::EPSILON);
        assert!((top - (800.0 - 69.0 - MENU_MARGIN)).abs() < f64::EPSILON);
    }
}
