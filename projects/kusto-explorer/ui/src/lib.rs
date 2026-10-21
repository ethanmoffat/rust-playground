//! Browser UI for kusto-explorer.

pub mod api;
pub mod blocks;
pub mod browser;
pub mod chart;
pub mod deeplink;
pub mod format;
pub mod fuzzy;
pub mod host;
pub mod keymap;
pub mod persist;
pub mod starters;
pub mod state;
pub mod time;
mod views;

use wasm_bindgen::prelude::wasm_bindgen;

pub use views::LOGO_SVG;

/// Starts the application once the WebAssembly module loads.
#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
    views::mount();
}
