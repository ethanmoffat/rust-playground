//! Commands, their palette entries, and keyboard shortcuts.

/// Every action reachable from the palette or the keyboard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Command {
    /// Run the selection or the query block at the cursor.
    Run,
    /// Cancel the running query.
    Cancel,
    /// Open a new query tab.
    NewTab,
    /// Close the active tab.
    CloseTab,
    /// Duplicate the active tab.
    DuplicateTab,
    /// Activate the next tab.
    NextTab,
    /// Activate the previous tab.
    PreviousTab,
    /// Open a `.kql` file.
    OpenFile,
    /// Save the active tab as a `.kql` file.
    SaveFile,
    /// Format the active query.
    Format,
    /// Copy a link that opens the current query.
    Share,
    /// Open the command palette.
    CommandPalette,
    /// Open quick open.
    QuickOpen,
    /// Show or hide the side bar.
    ToggleSidebar,
    /// Show the connections view.
    ShowConnections,
    /// Show the history view.
    ShowHistory,
    /// Show the starter queries view.
    ShowStarters,
    /// Show the settings view.
    ShowSettings,
    /// Switch between dark and light themes.
    ToggleTheme,
    /// Use the dark theme.
    ThemeDark,
    /// Use the light theme.
    ThemeLight,
    /// Follow the operating system theme.
    ThemeSystem,
    /// Increase the editor and grid font size.
    ZoomIn,
    /// Decrease the editor and grid font size.
    ZoomOut,
    /// Reset the font size.
    ZoomReset,
    /// Show keyboard shortcuts.
    Shortcuts,
    /// Reload databases and schemas.
    RefreshSchema,
    /// Maximize or restore the results panel.
    ToggleResults,
    /// Restore the default layout.
    ResetLayout,
    /// Focus the query editor.
    FocusEditor,
    /// Download the current result table as CSV.
    ExportCsv,
    /// Download the current result table as JSON.
    ExportJson,
    /// Download the current result table as Markdown.
    ExportMarkdown,
    /// Copy the current result table as CSV.
    CopyCsv,
    /// Copy the current result table as Markdown.
    CopyMarkdown,
    /// Copy the current result table as a KQL `datatable`.
    CopyDatatable,
    /// Remove every history entry.
    ClearHistory,
}

/// Palette metadata for a command.
#[derive(Clone, Copy, Debug)]
pub struct CommandInfo {
    /// The command.
    pub command: Command,
    /// Category prefix shown in the palette.
    pub category: &'static str,
    /// Title.
    pub title: &'static str,
    /// Shortcut label.
    pub keys: &'static str,
    /// Codicon name.
    pub icon: &'static str,
}

const fn info(
    command: Command,
    category: &'static str,
    title: &'static str,
    keys: &'static str,
    icon: &'static str,
) -> CommandInfo {
    CommandInfo {
        command,
        category,
        title,
        keys,
        icon,
    }
}

/// Commands in palette order.
pub const COMMANDS: &[CommandInfo] = &[
    info(Command::Run, "Query", "Run query", "F5", "play"),
    info(Command::Cancel, "Query", "Cancel query", "Shift+F5", "debug-stop"),
    info(Command::Format, "Query", "Format query", "Shift+Alt+F", "list-flat"),
    info(Command::Share, "Query", "Copy link to query", "", "link"),
    info(Command::NewTab, "Tabs", "New query tab", "Alt+N", "add"),
    info(Command::CloseTab, "Tabs", "Close tab", "Alt+W", "close"),
    info(Command::DuplicateTab, "Tabs", "Duplicate tab", "", "copy"),
    info(Command::NextTab, "Tabs", "Next tab", "Alt+PageDown", "arrow-right"),
    info(Command::PreviousTab, "Tabs", "Previous tab", "Alt+PageUp", "arrow-left"),
    info(Command::OpenFile, "File", "Open .kql file…", "Ctrl+O", "folder-opened"),
    info(Command::SaveFile, "File", "Save as .kql file", "Ctrl+S", "save"),
    info(Command::ExportCsv, "Results", "Export as CSV", "", "export"),
    info(Command::ExportJson, "Results", "Export as JSON", "", "json"),
    info(Command::ExportMarkdown, "Results", "Export as Markdown", "", "markdown"),
    info(Command::CopyCsv, "Results", "Copy as CSV", "", "copy"),
    info(Command::CopyMarkdown, "Results", "Copy as Markdown table", "", "copy"),
    info(Command::CopyDatatable, "Results", "Copy as KQL datatable", "", "copy"),
    info(
        Command::ToggleResults,
        "View",
        "Maximize or restore results",
        "",
        "screen-full",
    ),
    info(
        Command::ToggleSidebar,
        "View",
        "Toggle side bar",
        "Ctrl+B",
        "layout-sidebar-left",
    ),
    info(Command::ShowConnections, "View", "Show connections", "", "database"),
    info(Command::ShowHistory, "View", "Show history", "", "history"),
    info(Command::ShowStarters, "View", "Show starter queries", "", "lightbulb"),
    info(Command::ShowSettings, "View", "Show settings", "", "settings-gear"),
    info(Command::FocusEditor, "View", "Focus query editor", "", "edit"),
    info(Command::ResetLayout, "View", "Reset layout", "", "layout"),
    info(Command::ToggleTheme, "Theme", "Toggle dark and light", "", "color-mode"),
    info(Command::ThemeDark, "Theme", "Dark", "", "color-mode"),
    info(Command::ThemeLight, "Theme", "Light", "", "color-mode"),
    info(Command::ThemeSystem, "Theme", "Follow system", "", "color-mode"),
    info(Command::ZoomIn, "View", "Zoom in", "Ctrl+=", "zoom-in"),
    info(Command::ZoomOut, "View", "Zoom out", "Ctrl+-", "zoom-out"),
    info(Command::ZoomReset, "View", "Reset zoom", "Ctrl+0", "screen-normal"),
    info(
        Command::RefreshSchema,
        "Connections",
        "Refresh databases and schemas",
        "",
        "refresh",
    ),
    info(Command::ClearHistory, "History", "Clear query history", "", "clear-all"),
    info(
        Command::CommandPalette,
        "Help",
        "Show all commands",
        "Ctrl+Shift+P",
        "terminal",
    ),
    info(
        Command::QuickOpen,
        "Help",
        "Go to tab, table, or query…",
        "Ctrl+P",
        "go-to-file",
    ),
    info(Command::Shortcuts, "Help", "Keyboard shortcuts", "Shift+F1", "keyboard"),
];

/// Extra shortcuts that are not the primary binding of a command.
pub const ALTERNATE_KEYS: &[(&str, &str)] = &[
    ("Run query", "Shift+Enter"),
    ("Run query", "Ctrl+Enter"),
    ("Show all commands", "F1"),
];

/// Looks up palette metadata.
#[must_use]
pub fn info_for(command: Command) -> Option<&'static CommandInfo> {
    COMMANDS.iter().find(|entry| entry.command == command)
}

/// The parts of a keyboard event used for matching.
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyInput<'a> {
    /// `KeyboardEvent.key`.
    pub key: &'a str,
    /// `KeyboardEvent.code`.
    pub code: &'a str,
    /// Control or Command.
    pub ctrl: bool,
    /// Shift.
    pub shift: bool,
    /// Alt or Option.
    pub alt: bool,
}

/// Maps a key press to a command.
#[must_use]
pub fn command_for(input: &KeyInput<'_>) -> Option<Command> {
    let KeyInput {
        key,
        code,
        ctrl,
        shift,
        alt,
    } = *input;
    match (ctrl, shift, alt) {
        (false, false, false) => match key {
            "F5" => Some(Command::Run),
            "F1" => Some(Command::CommandPalette),
            _ => None,
        },
        (false, true, false) => match key {
            "F5" => Some(Command::Cancel),
            "F1" => Some(Command::Shortcuts),
            "Enter" => Some(Command::Run),
            _ => None,
        },
        (true, false, false) => match (key, code) {
            ("Enter", _) => Some(Command::Run),
            (_, "KeyO") => Some(Command::OpenFile),
            (_, "KeyS") => Some(Command::SaveFile),
            (_, "KeyP") => Some(Command::QuickOpen),
            (_, "KeyB") => Some(Command::ToggleSidebar),
            ("=" | "+", _) | (_, "Equal" | "NumpadAdd") => Some(Command::ZoomIn),
            ("-", _) | (_, "Minus" | "NumpadSubtract") => Some(Command::ZoomOut),
            (_, "Digit0" | "Numpad0") => Some(Command::ZoomReset),
            _ => None,
        },
        (true, true, false) => match (key, code) {
            (_, "KeyP") => Some(Command::CommandPalette),
            ("+", _) | (_, "Equal") => Some(Command::ZoomIn),
            _ => None,
        },
        (false, false, true) => match (key, code) {
            (_, "KeyN") => Some(Command::NewTab),
            (_, "KeyW") => Some(Command::CloseTab),
            ("PageDown", _) => Some(Command::NextTab),
            ("PageUp", _) => Some(Command::PreviousTab),
            _ => None,
        },
        (false, true, true) => match code {
            "KeyF" => Some(Command::Format),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press(key: &str, code: &str, ctrl: bool, shift: bool, alt: bool) -> Option<Command> {
        command_for(&KeyInput {
            key,
            code,
            ctrl,
            shift,
            alt,
        })
    }

    #[test]
    fn run_and_cancel_bindings() {
        assert_eq!(press("F5", "F5", false, false, false), Some(Command::Run));
        assert_eq!(press("Enter", "Enter", false, true, false), Some(Command::Run));
        assert_eq!(press("Enter", "Enter", true, false, false), Some(Command::Run));
        assert_eq!(press("F5", "F5", false, true, false), Some(Command::Cancel));
        assert_eq!(press("Enter", "Enter", false, false, false), None);
    }

    #[test]
    fn layout_independent_letters_and_editor_keys_are_left_alone() {
        assert_eq!(press("˜", "KeyN", false, false, true), Some(Command::NewTab));
        assert_eq!(press("P", "KeyP", true, true, false), Some(Command::CommandPalette));
        assert_eq!(press("p", "KeyP", true, false, false), Some(Command::QuickOpen));
        assert_eq!(press("F", "KeyF", false, true, true), Some(Command::Format));
        assert_eq!(press("/", "Slash", true, false, false), None);
        assert_eq!(press("L", "KeyL", true, true, false), None);
        assert_eq!(press("c", "KeyC", true, false, false), None);
    }

    #[test]
    fn every_bound_command_has_palette_metadata() {
        for command in [
            Command::Run,
            Command::Cancel,
            Command::NewTab,
            Command::Format,
            Command::ZoomIn,
            Command::Shortcuts,
        ] {
            assert!(info_for(command).is_some());
        }
        let titles: std::collections::HashSet<&str> = COMMANDS.iter().map(|entry| entry.title).collect();
        assert_eq!(titles.len(), COMMANDS.len());
        assert!(ALTERNATE_KEYS.iter().all(|(title, _)| titles.contains(title)));
    }
}
