//! Thin wrappers over browser APIs.

use std::{fmt::Write as _, time::Duration};

use leptos::prelude::{document, set_timeout, window};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_futures::JsFuture;

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn now() -> f64 {
    js_sys::Date::now()
}

/// Reads a `localStorage` value.
#[must_use]
pub fn storage_get(key: &str) -> Option<String> {
    window().local_storage().ok().flatten()?.get_item(key).ok().flatten()
}

/// Writes a `localStorage` value, ignoring quota and privacy-mode failures.
pub fn storage_set(key: &str, value: &str) {
    if let Some(storage) = window().local_storage().ok().flatten() {
        let _ = storage.set_item(key, value);
    }
}

/// A random version 4 UUID; works outside secure contexts.
#[must_use]
pub fn uuid() -> String {
    let mut bytes = [0u8; 16];
    let filled = window()
        .crypto()
        .ok()
        .is_some_and(|crypto| crypto.get_random_values_with_u8_array(&mut bytes).is_ok());
    if !filled {
        for byte in &mut bytes {
            *byte = (js_sys::Math::random() * 256.0) as u8;
        }
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let mut out = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Copies `text` to the clipboard, falling back to a selection copy outside secure contexts.
pub async fn copy_text(text: String) -> bool {
    let navigator = window().navigator();
    let has_clipboard = js_sys::Reflect::get(&navigator, &JsValue::from_str("clipboard"))
        .is_ok_and(|value| !value.is_undefined() && !value.is_null());
    if has_clipboard
        && window().is_secure_context()
        && JsFuture::from(navigator.clipboard().write_text(&text)).await.is_ok()
    {
        return true;
    }
    copy_with_selection(&text)
}

fn copy_with_selection(text: &str) -> bool {
    let document = document();
    let Some(body) = document.body() else { return false };
    let Ok(element) = document.create_element("textarea") else {
        return false;
    };
    let area: web_sys::HtmlTextAreaElement = element.unchecked_into();
    let previous = document.active_element();
    area.set_value(text);
    let _ = area.set_attribute("readonly", "");
    let _ = area.set_attribute("aria-hidden", "true");
    let _ = area.set_attribute("style", "position:fixed;top:0;left:-10000px;opacity:0");
    let _ = body.append_child(&area);
    area.select();
    let copied = document
        .unchecked_ref::<web_sys::HtmlDocument>()
        .exec_command("copy")
        .unwrap_or(false);
    area.remove();
    if let Some(previous) = previous.and_then(|element| element.dyn_into::<web_sys::HtmlElement>().ok()) {
        let _ = previous.focus();
    }
    copied
}

/// Starts a download of `content` as `file_name`.
pub fn download(file_name: &str, mime: &str, content: &str) -> bool {
    let parts = js_sys::Array::of1(&JsValue::from_str(content));
    let options = web_sys::BlobPropertyBag::new();
    options.set_type(mime);
    let Ok(blob) = web_sys::Blob::new_with_str_sequence_and_options(&parts, &options) else {
        return false;
    };
    let Ok(url) = web_sys::Url::create_object_url_with_blob(&blob) else {
        return false;
    };
    let document = document();
    let (Some(body), Ok(element)) = (document.body(), document.create_element("a")) else {
        return false;
    };
    let anchor: web_sys::HtmlAnchorElement = element.unchecked_into();
    anchor.set_href(&url);
    anchor.set_download(file_name);
    let _ = anchor.set_attribute("hidden", "");
    let _ = body.append_child(&anchor);
    anchor.click();
    anchor.remove();
    set_timeout(
        move || drop(web_sys::Url::revoke_object_url(&url)),
        Duration::from_secs(60),
    );
    true
}

/// Lets the user pick a text file and passes its name and content to `on_load`.
pub fn pick_text_file(accept: &str, on_load: impl FnOnce(String, String) + 'static) {
    let Ok(element) = document().create_element("input") else {
        return;
    };
    let input: web_sys::HtmlInputElement = element.unchecked_into();
    input.set_type("file");
    input.set_accept(accept);
    let source = input.clone();
    let on_change = Closure::once_into_js(move || {
        let Some(file) = source.files().and_then(|files| files.get(0)) else {
            return;
        };
        let name = file.name();
        wasm_bindgen_futures::spawn_local(async move {
            if let Ok(text) = JsFuture::from(file.text()).await {
                on_load(name, text.as_string().unwrap_or_default());
            }
        });
    });
    input.set_onchange(Some(on_change.unchecked_ref()));
    input.click();
}

/// Whether the operating system prefers a dark theme.
#[must_use]
pub fn prefers_dark() -> bool {
    window()
        .match_media("(prefers-color-scheme: dark)")
        .ok()
        .flatten()
        .is_none_or(|query| query.matches())
}

/// Calls `listener` whenever the operating system theme changes.
pub fn on_prefers_dark_change(listener: impl Fn(bool) + 'static) {
    let Some(query) = window().match_media("(prefers-color-scheme: dark)").ok().flatten() else {
        return;
    };
    let source = query.clone();
    let callback = Closure::<dyn Fn()>::new(move || listener(source.matches()));
    let _ = query.add_event_listener_with_callback("change", callback.as_ref().unchecked_ref());
    callback.forget();
}

/// Sets an attribute on the root `<html>` element.
pub fn set_root_attribute(name: &str, value: &str) {
    if let Some(root) = document().document_element() {
        let _ = root.set_attribute(name, value);
    }
}

/// Sets a CSS custom property on the root `<html>` element.
pub fn set_root_property(name: &str, value: &str) {
    if let Some(root) = document()
        .document_element()
        .and_then(|root| root.dyn_into::<web_sys::HtmlElement>().ok())
    {
        let _ = root.style().set_property(name, value);
    }
}

/// Removes and returns the URL fragment without adding a history entry.
#[must_use]
pub fn take_fragment() -> Option<String> {
    let location = window().location();
    let hash = location.hash().ok().filter(|hash| hash.len() > 1)?;
    let path = format!(
        "{}{}",
        location.pathname().unwrap_or_default(),
        location.search().unwrap_or_default()
    );
    if let Ok(history) = window().history() {
        let _ = history.replace_state_with_url(&JsValue::NULL, "", Some(&path));
    }
    Some(hash)
}

/// The page URL without query string or fragment, with `fragment` appended.
#[must_use]
pub fn link_with_fragment(fragment: &str) -> String {
    let location = window().location();
    format!(
        "{}{}#{fragment}",
        location.origin().unwrap_or_default(),
        location.pathname().unwrap_or_default()
    )
}

/// Moves keyboard focus to the first element matching `selector`.
pub fn focus_selector(selector: &str) {
    if let Some(element) = document()
        .query_selector(selector)
        .ok()
        .flatten()
        .and_then(|element| element.dyn_into::<web_sys::HtmlElement>().ok())
    {
        let _ = element.focus();
    }
}

/// Whether keyboard focus is inside an element matching `selector`.
#[must_use]
pub fn focus_within(selector: &str) -> bool {
    document()
        .active_element()
        .is_some_and(|element| element.closest(selector).ok().flatten().is_some())
}

/// Reports an unexpected problem to the developer console.
pub fn console_warn(message: &str) {
    web_sys::console::warn_1(&JsValue::from_str(message));
}

/// The `message` of a JavaScript error, or its string form.
#[must_use]
pub fn js_message(value: &JsValue) -> String {
    js_sys::Reflect::get(value, &JsValue::from_str("message"))
        .ok()
        .and_then(|message| message.as_string())
        .or_else(|| value.as_string())
        .unwrap_or_else(|| format!("{value:?}"))
}

/// The `name` of a JavaScript error, such as `AbortError`.
#[must_use]
pub fn js_name(value: &JsValue) -> Option<String> {
    js_sys::Reflect::get(value, &JsValue::from_str("name"))
        .ok()
        .and_then(|name| name.as_string())
}

/// Keeps a resize observer connected until dropped.
pub struct SizeObserver {
    observer: web_sys::ResizeObserver,
    _callback: Closure<dyn FnMut(js_sys::Array)>,
}

impl Drop for SizeObserver {
    fn drop(&mut self) {
        self.observer.disconnect();
    }
}

/// Calls `listener` with the content size of `element` now and whenever it changes.
pub fn observe_size(element: &web_sys::Element, listener: impl Fn(f64, f64) + 'static) -> Option<SizeObserver> {
    let callback = Closure::<dyn FnMut(js_sys::Array)>::new(move |entries: js_sys::Array| {
        if let Ok(entry) = entries.get(0).dyn_into::<web_sys::ResizeObserverEntry>() {
            let rect = entry.content_rect();
            listener(rect.width(), rect.height());
        }
    });
    let observer = web_sys::ResizeObserver::new(callback.as_ref().unchecked_ref()).ok()?;
    observer.observe(element);
    Some(SizeObserver {
        observer,
        _callback: callback,
    })
}

/// Width and height of the browser viewport.
#[must_use]
pub fn viewport_size() -> (f64, f64) {
    let window = window();
    let size = |value: Result<JsValue, JsValue>| value.ok().and_then(|value| value.as_f64()).unwrap_or(1024.0);
    (size(window.inner_width()), size(window.inner_height()))
}

/// Sets the document title.
pub fn set_title(title: &str) {
    document().set_title(title);
}

/// Scrolls the first element matching `selector` into view if it is outside its scroll container.
pub fn scroll_into_view(selector: &str) {
    if let Some(element) = document().query_selector(selector).ok().flatten() {
        let options = web_sys::ScrollIntoViewOptions::new();
        options.set_block(web_sys::ScrollLogicalPosition::Nearest);
        options.set_inline(web_sys::ScrollLogicalPosition::Nearest);
        element.scroll_into_view_with_scroll_into_view_options(&options);
    }
}
