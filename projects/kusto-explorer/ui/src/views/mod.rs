//! The user interface: a workbench with an activity bar, side bar, editor, results panel, and status bar.

mod chrome;
mod editor;
mod grid;
mod overlays;
mod results;
mod sidebar;

use leptos::prelude::*;
use wasm_bindgen::{JsCast, closure::Closure};

use self::{
    chrome::{ActivityBar, StatusBar, TitleBar},
    editor::EditorGroup,
    overlays::Overlays,
    sidebar::Sidebar,
};
use crate::{
    browser, host,
    keymap::{self, Command, KeyInput},
    state::{App, Menu, MenuEntry, Overlay},
};

/// The application mark: a query prompt over a result row.
pub const LOGO_SVG: &str = r##"<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><defs><linearGradient id="kx-logo-fill" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#3d9bff"/><stop offset="1" stop-color="#7a5cff"/></linearGradient></defs><rect x="1" y="1" width="22" height="22" rx="6" fill="url(#kx-logo-fill)"/><path d="M6.5 7.5 10 10.5l-3.5 3" fill="none" stroke="#fff" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"/><path d="M11.5 13.5h6" stroke="#fff" stroke-width="1.9" stroke-linecap="round"/><path d="M6.5 17.5h11" stroke="#fff" stroke-opacity=".55" stroke-width="1.9" stroke-linecap="round"/></svg>"##;

/// Renders a codicon; `name` may carry modifiers such as `loading codicon-modifier-spin`.
pub(crate) fn icon(name: &str) -> AnyView {
    view! { <i class=format!("codicon codicon-{name}") aria-hidden="true"></i> }.into_any()
}

/// Renders a shortcut such as `Ctrl+Shift+P` as key caps.
pub(crate) fn keys(shortcut: &str) -> AnyView {
    let mut parts: Vec<String> = Vec::new();
    for part in shortcut.split('+') {
        match parts.last_mut() {
            Some(last) if part.is_empty() && last.is_empty() => "+".clone_into(last),
            _ => parts.push(part.to_owned()),
        }
    }
    parts.retain(|part| !part.is_empty());
    view! { <span class="kx-keys">{parts.into_iter().map(|part| view! { <kbd>{part}</kbd> }).collect_view()}</span> }
        .into_any()
}

/// Opens a context menu at the pointer.
pub(crate) fn open_menu(app: App, event: &web_sys::MouseEvent, entries: Vec<MenuEntry>) {
    event.prevent_default();
    event.stop_propagation();
    app.menu.set(Some(Menu {
        x: f64::from(event.client_x()),
        y: f64::from(event.client_y()),
        entries,
    }));
}

/// Opens a menu below the element that received `event`.
pub(crate) fn open_menu_below(app: App, event: &web_sys::MouseEvent, entries: Vec<MenuEntry>) {
    event.stop_propagation();
    let Some(element) = event
        .current_target()
        .and_then(|target| target.dyn_into::<web_sys::Element>().ok())
    else {
        return;
    };
    let rect = element.get_bounding_client_rect();
    app.menu.set(Some(Menu {
        x: rect.left(),
        y: rect.bottom() + 2.0,
        entries,
    }));
}

/// Mounts the application into `#app`.
pub fn mount() {
    let Some(root) = document().get_element_by_id("app") else {
        browser::console_warn("kusto-explorer: the #app element is missing");
        return;
    };
    root.set_inner_html("");
    let _ = root.class_list().remove_1("kx-boot");
    leptos::mount::mount_to(root.unchecked_into(), || {
        let app = App::restore();
        provide_context(app);
        app.start();
        install_keyboard(app);
        Effect::new(move |_| {
            let title = app.active_tab().map(|tab| tab.title.get()).unwrap_or_default();
            browser::set_title(&if title.is_empty() {
                "kusto-explorer".to_owned()
            } else {
                format!("{title} · kusto-explorer")
            });
        });
        view! { <Shell/> }
    })
    .forget();
}

fn install_keyboard(app: App) {
    let listener = Closure::<dyn FnMut(web_sys::KeyboardEvent)>::new(move |event: web_sys::KeyboardEvent| {
        if event.is_composing() {
            return;
        }
        let key = event.key();
        if key == "Escape" {
            if app.menu.with_untracked(Option::is_some) {
                app.menu.set(None);
                consume(&event);
            } else if app.overlay.with_untracked(|overlay| *overlay != Overlay::None) {
                app.overlay.set(Overlay::None);
                host::focus();
                consume(&event);
            }
            return;
        }
        let code = event.code();
        let input = KeyInput {
            key: &key,
            code: &code,
            ctrl: event.ctrl_key() || event.meta_key(),
            shift: event.shift_key(),
            alt: event.alt_key(),
        };
        let Some(command) = keymap::command_for(&input) else {
            return;
        };
        let overlay_open = app.overlay.with_untracked(|overlay| *overlay != Overlay::None);
        if overlay_open
            && !matches!(
                command,
                Command::CommandPalette | Command::QuickOpen | Command::Shortcuts
            )
        {
            return;
        }
        consume(&event);
        if event.repeat()
            && !matches!(
                command,
                Command::ZoomIn | Command::ZoomOut | Command::NextTab | Command::PreviousTab
            )
        {
            return;
        }
        app.menu.set(None);
        app.dispatch(command);
    });
    let _ = window().add_event_listener_with_callback_and_bool("keydown", listener.as_ref().unchecked_ref(), true);
    listener.forget();
}

fn consume(event: &web_sys::KeyboardEvent) {
    event.prevent_default();
    event.stop_propagation();
}

#[component]
fn Shell() -> impl IntoView {
    view! {
        <div class="kx-shell">
            <TitleBar/>
            <div class="kx-main">
                <ActivityBar/>
                <Sidebar/>
                <EditorGroup/>
            </div>
            <StatusBar/>
            <Overlays/>
        </div>
    }
}
