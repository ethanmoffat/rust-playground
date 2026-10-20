//! Settings, layout, tabs, and history that survive reloads.

use kusto_explorer_api::{DEFAULT_ROW_CAP, MAX_ROW_CAP, QueryKind};
use serde::{Deserialize, Serialize};

/// `localStorage` key of the workspace state.
pub const STATE_KEY: &str = "kx.v1.state";
/// `localStorage` key of the query history.
pub const HISTORY_KEY: &str = "kx.v1.history";
/// Maximum number of history entries kept.
pub const MAX_HISTORY: usize = 200;
/// Maximum characters of query text kept per history entry.
pub const MAX_HISTORY_QUERY_CHARS: usize = 20_000;
/// Default editor font size.
pub const DEFAULT_FONT_SIZE: u32 = 14;
/// Smallest editor font size.
pub const MIN_FONT_SIZE: u32 = 9;
/// Largest editor font size.
pub const MAX_FONT_SIZE: u32 = 28;
/// Smallest side bar width.
pub const MIN_SIDEBAR_WIDTH: f64 = 180.0;
/// Largest side bar width.
pub const MAX_SIDEBAR_WIDTH: f64 = 640.0;

/// Theme preference.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeChoice {
    /// Dark.
    #[default]
    Dark,
    /// Light.
    Light,
    /// Follow the operating system.
    System,
}

/// Views of the side bar.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SideView {
    /// Connections tree.
    #[default]
    Connections,
    /// Query history.
    History,
    /// Starter queries.
    Starters,
    /// Settings.
    Settings,
}

impl SideView {
    /// Every view in activity bar order.
    pub const ALL: [Self; 4] = [Self::Connections, Self::History, Self::Starters, Self::Settings];

    /// Title.
    #[must_use]
    pub fn title(self) -> &'static str {
        match self {
            Self::Connections => "Connections",
            Self::History => "History",
            Self::Starters => "Starter queries",
            Self::Settings => "Settings",
        }
    }

    /// Codicon name.
    #[must_use]
    pub fn icon(self) -> &'static str {
        match self {
            Self::Connections => "database",
            Self::History => "history",
            Self::Starters => "lightbulb",
            Self::Settings => "settings-gear",
        }
    }

    /// Stable identifier.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::Connections => "connections",
            Self::History => "history",
            Self::Starters => "starters",
            Self::Settings => "settings",
        }
    }
}

/// User preferences.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Prefs {
    /// Theme.
    pub theme: ThemeChoice,
    /// Editor font size in pixels.
    pub font_size: u32,
    /// Maximum rows per result table.
    pub row_cap: usize,
}

impl Default for Prefs {
    fn default() -> Self {
        Self {
            theme: ThemeChoice::Dark,
            font_size: DEFAULT_FONT_SIZE,
            row_cap: DEFAULT_ROW_CAP,
        }
    }
}

/// Window layout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Layout {
    /// Whether the side bar is visible.
    pub sidebar_visible: bool,
    /// Side bar width in pixels.
    pub sidebar_width: f64,
    /// Share of the editor area used by results.
    pub results_ratio: f64,
    /// Whether results fill the editor area.
    pub results_maximized: bool,
    /// Active side bar view.
    pub view: SideView,
}

impl Default for Layout {
    fn default() -> Self {
        Self {
            sidebar_visible: true,
            sidebar_width: 300.0,
            results_ratio: 0.46,
            results_maximized: false,
            view: SideView::Connections,
        }
    }
}

/// A persisted query tab.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SavedTab {
    /// Identifier unique within the workspace.
    pub id: u32,
    /// Title.
    pub title: String,
    /// Query text.
    pub text: String,
    /// Connection identifier.
    pub connection_id: String,
    /// Database.
    pub database: String,
    /// File name when opened from or saved to a file.
    pub file_name: Option<String>,
}

/// The persisted workspace.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SavedState {
    /// Open tabs.
    pub tabs: Vec<SavedTab>,
    /// Active tab identifier.
    pub active_tab: u32,
    /// Next tab identifier.
    pub next_tab: u32,
    /// Preferences.
    pub prefs: Prefs,
    /// Layout.
    pub layout: Layout,
    /// Tree nodes whose expansion differs from the default.
    pub toggled: Vec<String>,
}

impl SavedState {
    /// Parses persisted JSON, falling back to defaults, and clamps every value.
    #[must_use]
    pub fn restore(json: Option<&str>) -> Self {
        let mut state: Self = json
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default();
        state.prefs.font_size = state.prefs.font_size.clamp(MIN_FONT_SIZE, MAX_FONT_SIZE);
        state.prefs.row_cap = if state.prefs.row_cap == 0 {
            DEFAULT_ROW_CAP
        } else {
            state.prefs.row_cap.min(MAX_ROW_CAP)
        };
        let layout = &mut state.layout;
        layout.sidebar_width = if layout.sidebar_width.is_finite() {
            layout.sidebar_width.clamp(MIN_SIDEBAR_WIDTH, MAX_SIDEBAR_WIDTH)
        } else {
            Layout::default().sidebar_width
        };
        layout.results_ratio = if layout.results_ratio.is_finite() {
            layout.results_ratio.clamp(0.12, 0.88)
        } else {
            Layout::default().results_ratio
        };
        let mut seen = std::collections::HashSet::new();
        state.tabs.retain(|tab| tab.id > 0 && seen.insert(tab.id));
        let highest = state.tabs.iter().map(|tab| tab.id).max().unwrap_or(0);
        state.next_tab = state.next_tab.max(highest + 1);
        if !state.tabs.iter().any(|tab| tab.id == state.active_tab) {
            state.active_tab = state.tabs.first().map_or(0, |tab| tab.id);
        }
        state
    }
}

/// One executed query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    /// Client request identifier.
    pub id: String,
    /// Start time in epoch milliseconds.
    pub at: f64,
    /// Connection identifier.
    pub connection_id: String,
    /// Database.
    pub database: String,
    /// Query text, possibly shortened.
    pub query: String,
    /// Kind of request.
    pub kind: QueryKind,
    /// Round-trip duration.
    pub duration_ms: f64,
    /// Rows returned before the row cap.
    pub rows: usize,
    /// Whether the request succeeded.
    pub ok: bool,
    /// Error message when it failed.
    pub error: Option<String>,
}

/// Parses persisted history, dropping anything unreadable.
#[must_use]
pub fn restore_history(json: Option<&str>) -> Vec<HistoryEntry> {
    let mut entries: Vec<HistoryEntry> = json
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();
    entries.truncate(MAX_HISTORY);
    entries
}

/// Adds `entry` to the front of `history`, keeping the newest [`MAX_HISTORY`] entries.
pub fn push_history(history: &mut Vec<HistoryEntry>, mut entry: HistoryEntry) {
    if entry.query.chars().count() > MAX_HISTORY_QUERY_CHARS {
        entry.query = entry.query.chars().take(MAX_HISTORY_QUERY_CHARS).collect();
    }
    history.insert(0, entry);
    history.truncate(MAX_HISTORY);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_clamps_and_repairs() {
        let state = SavedState::restore(Some(
            r#"{"tabs":[{"id":3,"title":"a"},{"id":3,"title":"dup"},{"id":0}],"activeTab":9,"nextTab":1,
                "prefs":{"fontSize":99,"rowCap":0,"theme":"light"},"layout":{"sidebarWidth":5,"resultsRatio":2}}"#,
        ));
        assert_eq!(state.tabs.len(), 1);
        assert_eq!(state.active_tab, 3);
        assert_eq!(state.next_tab, 4);
        assert_eq!(state.prefs.font_size, MAX_FONT_SIZE);
        assert_eq!(state.prefs.row_cap, DEFAULT_ROW_CAP);
        assert_eq!(state.prefs.theme, ThemeChoice::Light);
        assert_eq!(state.layout.sidebar_width, MIN_SIDEBAR_WIDTH);
        assert!((state.layout.results_ratio - 0.88).abs() < f64::EPSILON);
        assert!(state.layout.sidebar_visible);
    }

    #[test]
    fn restore_tolerates_garbage() {
        assert_eq!(SavedState::restore(Some("{not json")).tabs.len(), 0);
        assert_eq!(SavedState::restore(None).prefs, Prefs::default());
        assert!(restore_history(Some("[1,2]")).is_empty());
    }

    #[test]
    fn history_is_bounded() {
        let mut history = Vec::new();
        for index in 0..(MAX_HISTORY + 5) {
            push_history(
                &mut history,
                HistoryEntry {
                    id: index.to_string(),
                    at: index as f64,
                    connection_id: "c".to_owned(),
                    database: "d".to_owned(),
                    query: "x".repeat(if index == 0 { MAX_HISTORY_QUERY_CHARS + 10 } else { 1 }),
                    kind: QueryKind::Query,
                    duration_ms: 1.0,
                    rows: 1,
                    ok: true,
                    error: None,
                },
            );
        }
        assert_eq!(history.len(), MAX_HISTORY);
        assert_eq!(history[0].id, (MAX_HISTORY + 4).to_string());
        let json = serde_json::to_string(&history).unwrap_or_default();
        assert_eq!(restore_history(Some(&json)).len(), MAX_HISTORY);
    }
}
