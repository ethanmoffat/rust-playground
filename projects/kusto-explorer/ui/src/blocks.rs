//! Query block detection: queries in a tab are separated by blank lines.

/// Returns the block of consecutive non-blank lines around the 1-based `line`.
///
/// When `line` is blank, the nearest block above wins, then the nearest block below.
#[must_use]
pub fn block_at(text: &str, line: usize) -> Option<String> {
    let lines: Vec<&str> = text
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect();
    let blank = |index: usize| lines.get(index).is_none_or(|line| line.trim().is_empty());
    let mut index = line.saturating_sub(1).min(lines.len().saturating_sub(1));
    if blank(index) {
        index = (0..index)
            .rev()
            .find(|candidate| !blank(*candidate))
            .or_else(|| (index + 1..lines.len()).find(|candidate| !blank(*candidate)))?;
    }
    let mut start = index;
    while start > 0 && !blank(start - 1) {
        start -= 1;
    }
    let mut end = index;
    while end + 1 < lines.len() && !blank(end + 1) {
        end += 1;
    }
    Some(lines[start..=end].join("\n"))
}

/// Returns a short single-line summary of a query for titles and lists.
#[must_use]
pub fn summary(query: &str, max_chars: usize) -> String {
    let line = query
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with("//"))
        .or_else(|| query.lines().map(str::trim).find(|line| !line.is_empty()))
        .unwrap_or_default();
    let mut summary: String = line.chars().take(max_chars).collect();
    if line.chars().count() > max_chars {
        summary.push('…');
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = "// first\nT | take 10\n\n\n.show tables\r\n| count\n\nlast";

    #[test]
    fn cursor_inside_a_block_selects_the_whole_block() {
        assert_eq!(block_at(TEXT, 1).as_deref(), Some("// first\nT | take 10"));
        assert_eq!(block_at(TEXT, 2).as_deref(), Some("// first\nT | take 10"));
        assert_eq!(block_at(TEXT, 6).as_deref(), Some(".show tables\n| count"));
        assert_eq!(block_at(TEXT, 8).as_deref(), Some("last"));
    }

    #[test]
    fn blank_lines_prefer_the_block_above() {
        assert_eq!(block_at(TEXT, 3).as_deref(), Some("// first\nT | take 10"));
        assert_eq!(block_at(TEXT, 7).as_deref(), Some(".show tables\n| count"));
        assert_eq!(block_at("\n\nT", 1).as_deref(), Some("T"));
        assert_eq!(block_at(TEXT, 99).as_deref(), Some("last"));
    }

    #[test]
    fn empty_text_has_no_block() {
        assert_eq!(block_at("", 1), None);
        assert_eq!(block_at("  \n\t\n", 2), None);
    }

    #[test]
    fn summary_skips_comments_and_truncates() {
        assert_eq!(summary("// note\n  StormEvents | take 10", 11), "StormEvents…");
        assert_eq!(summary("// only", 40), "// only");
        assert_eq!(summary("", 10), "");
    }
}
