//! Date, time, and number formatting without a browser.

const MS_PER_SECOND: f64 = 1_000.0;
const MS_PER_MINUTE: f64 = 60.0 * MS_PER_SECOND;
const MS_PER_HOUR: f64 = 60.0 * MS_PER_MINUTE;
const MS_PER_DAY: f64 = 24.0 * MS_PER_HOUR;

/// Parses an ISO-8601 timestamp such as `2026-01-02T03:04:05.1234567Z` into epoch milliseconds.
#[must_use]
pub fn parse_datetime_ms(text: &str) -> Option<f64> {
    let text = text.trim();
    let bytes = text.as_bytes();
    if bytes.len() < 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| -> Option<i64> { text.get(range)?.parse().ok() };
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut millis = days_from_civil(year, month, day) as f64 * MS_PER_DAY;
    if bytes.len() == 10 {
        return Some(millis);
    }
    if !matches!(bytes[10], b'T' | b' ') || bytes.len() < 16 {
        return None;
    }
    let (hour, minute) = (number(11..13)?, number(14..16)?);
    millis += hour as f64 * MS_PER_HOUR + minute as f64 * MS_PER_MINUTE;
    let mut rest = &text[16..];
    if let Some(tail) = rest.strip_prefix(':') {
        let seconds: f64 = tail.get(0..2)?.parse().ok()?;
        millis += seconds * MS_PER_SECOND;
        rest = &tail[2..];
        if let Some(fraction) = rest.strip_prefix('.') {
            let digits = fraction.chars().take_while(char::is_ascii_digit).count();
            if digits > 0 {
                let value: f64 = format!("0.{}", &fraction[..digits]).parse().ok()?;
                millis += value * MS_PER_SECOND;
            }
            rest = &fraction[digits..];
        }
    }
    match rest {
        "" | "Z" | "z" => Some(millis),
        offset if offset.len() == 6 && matches!(offset.as_bytes()[0], b'+' | b'-') => {
            let hours: f64 = offset.get(1..3)?.parse().ok()?;
            let minutes: f64 = offset.get(4..6)?.parse().ok()?;
            let shift = hours * MS_PER_HOUR + minutes * MS_PER_MINUTE;
            Some(if offset.starts_with('+') {
                millis - shift
            } else {
                millis + shift
            })
        }
        _ => None,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
#[must_use]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Calendar date for days since 1970-01-01.
#[must_use]
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Formats epoch milliseconds as `YYYY-MM-DD HH:MM:SS` (UTC), trimming parts finer than `step_ms`.
#[must_use]
pub fn format_timestamp(millis: f64, step_ms: f64) -> String {
    let days = (millis / MS_PER_DAY).floor();
    let (year, month, day) = civil_from_days(days as i64);
    let in_day = millis - days * MS_PER_DAY;
    let hour = (in_day / MS_PER_HOUR).floor();
    let minute = ((in_day - hour * MS_PER_HOUR) / MS_PER_MINUTE).floor();
    let second = ((in_day - hour * MS_PER_HOUR - minute * MS_PER_MINUTE) / MS_PER_SECOND).floor();
    if step_ms >= MS_PER_DAY {
        format!("{year:04}-{month:02}-{day:02}")
    } else if step_ms >= MS_PER_MINUTE {
        format!("{month:02}-{day:02} {hour:02}:{minute:02}")
    } else {
        format!("{hour:02}:{minute:02}:{second:02}")
    }
}

/// Picks a readable tick interval so that `span_ms` holds roughly `ticks` ticks.
#[must_use]
pub fn nice_time_step(span_ms: f64, ticks: f64) -> f64 {
    const STEPS: [f64; 26] = [
        1.0,
        10.0,
        100.0,
        MS_PER_SECOND,
        2.0 * MS_PER_SECOND,
        5.0 * MS_PER_SECOND,
        10.0 * MS_PER_SECOND,
        15.0 * MS_PER_SECOND,
        30.0 * MS_PER_SECOND,
        MS_PER_MINUTE,
        2.0 * MS_PER_MINUTE,
        5.0 * MS_PER_MINUTE,
        10.0 * MS_PER_MINUTE,
        15.0 * MS_PER_MINUTE,
        30.0 * MS_PER_MINUTE,
        MS_PER_HOUR,
        2.0 * MS_PER_HOUR,
        3.0 * MS_PER_HOUR,
        6.0 * MS_PER_HOUR,
        12.0 * MS_PER_HOUR,
        MS_PER_DAY,
        2.0 * MS_PER_DAY,
        7.0 * MS_PER_DAY,
        14.0 * MS_PER_DAY,
        30.0 * MS_PER_DAY,
        365.0 * MS_PER_DAY,
    ];
    let raw = span_ms / ticks.max(1.0);
    STEPS
        .into_iter()
        .find(|step| *step >= raw)
        .unwrap_or(365.0 * MS_PER_DAY)
}

/// Formats a duration in milliseconds such as `850 ms`, `2.4 s`, or `1 m 05 s`.
#[must_use]
pub fn format_duration(millis: f64) -> String {
    if !millis.is_finite() || millis < 0.0 {
        return "—".to_owned();
    }
    if millis < MS_PER_SECOND {
        format!("{millis:.0} ms")
    } else if millis < MS_PER_MINUTE {
        format!("{:.1} s", millis / MS_PER_SECOND)
    } else {
        let total = (millis / MS_PER_SECOND).round() as u64;
        format!("{} m {:02} s", total / 60, total % 60)
    }
}

/// Formats a byte count with binary units.
#[must_use]
pub fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Formats an integer with thousands separators.
#[must_use]
pub fn format_count(count: u64) -> String {
    let digits = count.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// Formats a chart value compactly, such as `1.5K` or `12M`.
#[must_use]
pub fn format_compact(value: f64) -> String {
    let magnitude = value.abs();
    let (scaled, suffix) = if magnitude >= 1e12 {
        (value / 1e12, "T")
    } else if magnitude >= 1e9 {
        (value / 1e9, "B")
    } else if magnitude >= 1e6 {
        (value / 1e6, "M")
    } else if magnitude >= 1e4 {
        (value / 1e3, "K")
    } else {
        (value, "")
    };
    let text = if scaled.fract().abs() < 1e-9 || scaled.abs() >= 100.0 {
        format!("{scaled:.0}")
    } else if scaled.abs() >= 10.0 {
        format!("{scaled:.1}")
    } else {
        format!("{scaled:.2}")
    };
    let text = if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        text
    };
    format!("{text}{suffix}")
}

/// Formats how long ago `then_ms` was relative to `now_ms`.
#[must_use]
pub fn format_ago(then_ms: f64, now_ms: f64) -> String {
    let seconds = ((now_ms - then_ms) / MS_PER_SECOND).max(0.0);
    if seconds < 45.0 {
        "just now".to_owned()
    } else if seconds < 3_600.0 {
        format!("{:.0} min ago", (seconds / 60.0).max(1.0))
    } else if seconds < 86_400.0 {
        format!("{:.0} h ago", seconds / 3_600.0)
    } else {
        format_timestamp(then_ms, MS_PER_DAY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kusto_timestamps() {
        assert_eq!(parse_datetime_ms("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(parse_datetime_ms("1970-01-02"), Some(MS_PER_DAY));
        assert_eq!(parse_datetime_ms("2000-03-01T00:00:01.5Z"), Some(951_868_801_500.0));
        assert_eq!(
            parse_datetime_ms("2026-10-19T23:59:00.1234567Z").map(|ms| ms.round()),
            Some(1_792_454_340_123.0)
        );
        assert_eq!(parse_datetime_ms("1970-01-01T01:00:00+01:00"), Some(0.0));
        assert_eq!(parse_datetime_ms("1970-01-01 00:01"), Some(MS_PER_MINUTE));
        assert_eq!(parse_datetime_ms("not a date"), None);
        assert_eq!(parse_datetime_ms("2026-13-01"), None);
    }

    #[test]
    fn civil_round_trips() {
        for days in [-719_468, -1, 0, 1, 11_016, 20_000, 2_932_896] {
            let (year, month, day) = civil_from_days(days);
            assert_eq!(days_from_civil(year, month, day), days);
        }
    }

    #[test]
    fn formats_values() {
        assert_eq!(format_timestamp(0.0, MS_PER_DAY), "1970-01-01");
        assert_eq!(format_timestamp(MS_PER_HOUR * 25.5, MS_PER_HOUR), "01-02 01:30");
        assert_eq!(format_timestamp(61_000.0, MS_PER_SECOND), "00:01:01");
        assert_eq!(format_duration(850.0), "850 ms");
        assert_eq!(format_duration(2_400.0), "2.4 s");
        assert_eq!(format_duration(65_000.0), "1 m 05 s");
        assert_eq!(format_bytes(512.0), "512 B");
        assert_eq!(format_bytes(1_536.0), "1.5 KiB");
        assert_eq!(format_count(1_234_567), "1,234,567");
        assert_eq!(format_count(12), "12");
        assert_eq!(format_compact(1_500.0), "1500");
        assert_eq!(format_compact(15_000.0), "15K");
        assert_eq!(format_compact(2_500_000.0), "2.5M");
        assert_eq!(format_compact(0.1234), "0.12");
        assert_eq!(format_compact(42.26), "42.3");
        assert_eq!(nice_time_step(60.0 * MS_PER_MINUTE, 6.0), 10.0 * MS_PER_MINUTE);
        assert_eq!(format_ago(0.0, 10_000.0), "just now");
        assert_eq!(format_ago(0.0, 7_200_000.0), "2 h ago");
    }
}
