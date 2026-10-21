//! Bindings to the query editor host that the page script registers before the UI starts.

#![allow(missing_docs)]

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = mount)]
    pub fn mount(element: &web_sys::HtmlElement);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = openModel)]
    pub fn open_model(tab_id: &str, text: &str);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = activate)]
    pub fn activate(tab_id: &str);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = closeModel)]
    pub fn close_model(tab_id: &str);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = getText)]
    pub fn get_text(tab_id: &str) -> String;

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = setText)]
    pub fn set_text(tab_id: &str, text: &str);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = insertText)]
    pub fn insert_text(text: &str);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = selectionText)]
    pub fn selection_text() -> String;

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = cursorLine)]
    pub fn cursor_line() -> u32;

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = dismissSuggestions)]
    pub fn dismiss_suggestions();

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = format)]
    pub fn format_document();

    #[wasm_bindgen(catch, js_namespace = __kxEditorHost, js_name = setSchema)]
    pub async fn set_schema(schema_json: &str, cluster_uri: &str, database: &str) -> Result<JsValue, JsValue>;

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = setTheme)]
    pub fn set_theme(dark: bool);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = setFontSize)]
    pub fn set_font_size(size: u32);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = focus)]
    pub fn focus();

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = layout)]
    pub fn layout();

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = onChange)]
    pub fn on_change(listener: &Closure<dyn FnMut(String, String)>);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = onCursor)]
    pub fn on_cursor(listener: &Closure<dyn FnMut(u32, u32, u32)>);

    #[wasm_bindgen(js_namespace = __kxEditorHost, js_name = onCommand)]
    pub fn on_command(listener: &Closure<dyn FnMut(String)>);
}
