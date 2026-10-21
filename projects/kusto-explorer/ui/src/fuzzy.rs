//! Fuzzy matching for the command palette and quick open.

/// A successful match: higher scores rank first, `positions` are matched char indices.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Match {
    /// Ranking score.
    pub score: i32,
    /// Indices of matched characters in the candidate.
    pub positions: Vec<usize>,
}

/// Matches `query` as a case-insensitive subsequence of `candidate`.
#[must_use]
pub fn fuzzy_match(query: &str, candidate: &str) -> Option<Match> {
    let needle: Vec<char> = query
        .chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect();
    if needle.is_empty() {
        return Some(Match {
            score: 0,
            positions: Vec::new(),
        });
    }
    let haystack: Vec<char> = candidate.chars().collect();
    let lower: Vec<char> = haystack.iter().map(|c| c.to_lowercase().next().unwrap_or(*c)).collect();
    let length_penalty = (haystack.len() as i32 - needle.len() as i32).max(0) / 8;
    (0..lower.len())
        .filter(|start| lower[*start] == needle[0])
        .filter_map(|start| match_from(&needle, &haystack, &lower, start))
        .max_by(|left, right| {
            left.score
                .cmp(&right.score)
                .then_with(|| right.positions.cmp(&left.positions))
        })
        .map(|found| Match {
            score: found.score - length_penalty,
            positions: found.positions,
        })
}

fn match_from(needle: &[char], haystack: &[char], lower: &[char], start: usize) -> Option<Match> {
    let mut positions = Vec::with_capacity(needle.len());
    let mut score = -(start as i32).min(10);
    let mut next = start;
    let mut previous: Option<usize> = None;
    for wanted in needle {
        let found = (next..lower.len()).find(|index| lower[*index] == *wanted)?;
        let at_word_start = found == 0
            || !haystack[found - 1].is_alphanumeric()
            || (haystack[found].is_uppercase() && !haystack[found - 1].is_uppercase());
        score += 1;
        match previous {
            Some(previous) if previous + 1 == found => score += 10,
            Some(previous) => score -= ((found - previous - 1) as i32).min(5) + 3,
            None => {}
        }
        if at_word_start {
            score += 8;
        }
        positions.push(found);
        previous = Some(found);
        next = found + 1;
    }
    Some(Match { score, positions })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_subsequences_case_insensitively() {
        let found = fuzzy_match("rq", "Run Query").unwrap_or(Match {
            score: 0,
            positions: vec![],
        });
        assert_eq!(found.positions, vec![0, 4]);
        assert!(fuzzy_match("xyz", "Run Query").is_none());
        assert_eq!(fuzzy_match("", "anything").map(|m| m.score), Some(0));
    }

    #[test]
    fn word_starts_and_runs_rank_higher() {
        let score = |query: &str, candidate: &str| fuzzy_match(query, candidate).map_or(i32::MIN, |m| m.score);
        assert!(score("tog", "Toggle Theme") > score("tog", "Stop logging"));
        assert!(score("dark", "Theme: Dark") > score("dark", "d a r k"));
        assert!(score("loadtest", "loadtest") > score("loadtest", "loadtest_extra_long_name"));
        assert_eq!(
            fuzzy_match("dark", "d: Theme Dark").map(|m| m.positions),
            Some(vec![9, 10, 11, 12])
        );
    }
}
