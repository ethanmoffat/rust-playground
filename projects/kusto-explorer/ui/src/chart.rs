//! Charts for `render` results: a pure data model and an SVG renderer.

use std::collections::HashMap;
use std::fmt::Write as _;

use kusto_explorer_api::{ResultTable, Visualization};

use crate::format::{as_number, display, is_numeric};
use crate::time::{format_compact, format_count, format_timestamp, nice_time_step, parse_datetime_ms};

const MAX_SERIES: usize = 40;
const MAX_CATEGORIES: usize = 400;
const MAX_SLICES: usize = 12;
const PALETTE_SIZE: usize = 10;

/// Chart kinds the renderer supports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChartKind {
    /// Vertical bars per category.
    Column,
    /// Horizontal bars per category.
    Bar,
    /// Lines over an ordered x-axis.
    Line,
    /// Lines over a time x-axis.
    Time,
    /// Filled areas.
    Area,
    /// Points.
    Scatter,
    /// Slices of a whole.
    Pie,
}

impl ChartKind {
    /// Every kind in menu order.
    pub const ALL: [Self; 7] = [
        Self::Column,
        Self::Bar,
        Self::Line,
        Self::Time,
        Self::Area,
        Self::Scatter,
        Self::Pie,
    ];

    /// Maps a `render` visualization name to a kind.
    #[must_use]
    pub fn from_render(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "columnchart" => Some(Self::Column),
            "barchart" => Some(Self::Bar),
            "linechart" => Some(Self::Line),
            "timechart" | "anomalychart" => Some(Self::Time),
            "areachart" | "stackedareachart" => Some(Self::Area),
            "scatterchart" => Some(Self::Scatter),
            "piechart" => Some(Self::Pie),
            _ => None,
        }
    }

    /// Stable identifier.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::Column => "column",
            Self::Bar => "bar",
            Self::Line => "line",
            Self::Time => "time",
            Self::Area => "area",
            Self::Scatter => "scatter",
            Self::Pie => "pie",
        }
    }

    /// Parses [`ChartKind::id`].
    #[must_use]
    pub fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.id() == id)
    }

    /// Display label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Column => "Column",
            Self::Bar => "Bar",
            Self::Line => "Line",
            Self::Time => "Time",
            Self::Area => "Area",
            Self::Scatter => "Scatter",
            Self::Pie => "Pie",
        }
    }

    /// Codicon name.
    #[must_use]
    pub fn icon(self) -> &'static str {
        match self {
            Self::Column | Self::Bar => "graph",
            Self::Line | Self::Time | Self::Area => "graph-line",
            Self::Scatter => "graph-scatter",
            Self::Pie => "pie-chart",
        }
    }
}

/// How multiple series share a category.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Stacking {
    /// Side by side or overlapping.
    #[default]
    Grouped,
    /// Stacked values.
    Stacked,
    /// Stacked to 100%.
    Stacked100,
}

/// What to draw, resolved from `render` properties or a user choice.
#[derive(Clone, Debug, PartialEq)]
pub struct Spec {
    /// Chart kind.
    pub kind: ChartKind,
    /// Stacking mode.
    pub stacking: Stacking,
    /// Optional title.
    pub title: Option<String>,
    /// Explicit x column.
    pub x_column: Option<String>,
    /// Explicit y columns.
    pub y_columns: Vec<String>,
    /// Explicit series columns.
    pub series: Vec<String>,
    /// X-axis title.
    pub x_title: Option<String>,
    /// Y-axis title.
    pub y_title: Option<String>,
    /// Whether to show a legend.
    pub legend: bool,
    /// Y-axis minimum.
    pub ymin: Option<f64>,
    /// Y-axis maximum.
    pub ymax: Option<f64>,
    /// Logarithmic y-axis.
    pub log: bool,
}

impl Spec {
    /// A spec with defaults for `kind`.
    #[must_use]
    pub fn new(kind: ChartKind) -> Self {
        Self {
            kind,
            stacking: Stacking::Grouped,
            title: None,
            x_column: None,
            y_columns: Vec::new(),
            series: Vec::new(),
            x_title: None,
            y_title: None,
            legend: true,
            ymin: None,
            ymax: None,
            log: false,
        }
    }

    /// Resolves the `render` properties, optionally forcing another kind.
    #[must_use]
    pub fn from_visualization(visualization: &Visualization, kind: Option<ChartKind>) -> Option<Self> {
        let kind = kind.or_else(|| ChartKind::from_render(&visualization.visualization))?;
        let stacking = match visualization.kind.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("stacked") => Stacking::Stacked,
            Some("stacked100") => Stacking::Stacked100,
            _ if visualization.visualization.eq_ignore_ascii_case("stackedareachart") => Stacking::Stacked,
            _ => Stacking::Grouped,
        };
        Some(Self {
            kind,
            stacking,
            title: visualization.title.clone().filter(|title| !title.is_empty()),
            x_column: visualization.x_column.clone().filter(|column| !column.is_empty()),
            y_columns: visualization.y_columns.clone(),
            series: visualization.series.clone(),
            x_title: visualization.x_title.clone().filter(|title| !title.is_empty()),
            y_title: visualization.y_title.clone().filter(|title| !title.is_empty()),
            legend: !visualization
                .legend
                .as_deref()
                .is_some_and(|legend| legend.eq_ignore_ascii_case("hidden")),
            ymin: visualization.ymin,
            ymax: visualization.ymax,
            log: visualization
                .y_scale
                .as_deref()
                .is_some_and(|scale| scale.eq_ignore_ascii_case("log")),
        })
    }
}

/// The x-axis of a chart.
#[derive(Clone, Debug, PartialEq)]
pub enum Axis {
    /// Discrete labels; point x values are indices.
    Category(Vec<String>),
    /// Epoch milliseconds.
    Time,
    /// Plain numbers.
    Number,
}

/// One plotted series.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    /// Legend name.
    pub name: String,
    /// `(x, y)` points ordered by x.
    pub points: Vec<(f64, f64)>,
}

/// A chart ready to render.
#[derive(Clone, Debug, PartialEq)]
pub struct Model {
    /// Spec the model was built from.
    pub spec: Spec,
    /// X-axis.
    pub axis: Axis,
    /// Series.
    pub series: Vec<Series>,
    /// X-axis title.
    pub x_label: String,
    /// Y-axis title.
    pub y_label: String,
    /// Notes about dropped data.
    pub notes: Vec<String>,
}

impl Model {
    /// Whether to show a legend.
    #[must_use]
    pub fn show_legend(&self) -> bool {
        self.spec.legend && (self.spec.kind == ChartKind::Pie || self.series.len() > 1)
    }

    /// Legend entries as `(name, color index)`.
    #[must_use]
    pub fn legend(&self) -> Vec<(String, usize)> {
        if self.spec.kind == ChartKind::Pie {
            let labels = match &self.axis {
                Axis::Category(labels) => labels.clone(),
                _ => Vec::new(),
            };
            let total: f64 = self
                .series
                .first()
                .map_or(0.0, |series| series.points.iter().map(|point| point.1.max(0.0)).sum());
            return labels
                .into_iter()
                .enumerate()
                .map(|(index, label)| {
                    let value = self
                        .series
                        .first()
                        .and_then(|series| series.points.get(index))
                        .map_or(0.0, |point| point.1);
                    let share = if total > 0.0 {
                        value.max(0.0) / total * 100.0
                    } else {
                        0.0
                    };
                    (format!("{label} ({share:.1}%)"), index % PALETTE_SIZE)
                })
                .collect();
        }
        self.series
            .iter()
            .enumerate()
            .map(|(index, series)| (series.name.clone(), index % PALETTE_SIZE))
            .collect()
    }
}

fn find_column(table: &ResultTable, name: &str) -> Option<usize> {
    table.columns.iter().position(|column| column.name == name).or_else(|| {
        table
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
    })
}

/// Builds a chart model from a result table.
///
/// # Errors
///
/// Returns a user-facing message when the table cannot be plotted.
pub fn build(table: &ResultTable, spec: &Spec) -> Result<Model, String> {
    let columns = &table.columns;
    if columns.is_empty() {
        return Err("The result has no columns to plot.".to_owned());
    }
    let type_of = |index: usize| columns[index].column_type.as_str();
    let x = match &spec.x_column {
        Some(name) => find_column(table, name).ok_or_else(|| format!("Column '{name}' was not found."))?,
        None => match spec.kind {
            ChartKind::Time | ChartKind::Line | ChartKind::Area | ChartKind::Scatter => (0..columns.len())
                .find(|index| type_of(*index) == "datetime")
                .or_else(|| (0..columns.len()).find(|index| is_numeric(type_of(*index))))
                .unwrap_or(0),
            ChartKind::Column | ChartKind::Bar | ChartKind::Pie => (0..columns.len())
                .find(|index| !is_numeric(type_of(*index)))
                .unwrap_or(0),
        },
    };
    let series_columns: Vec<usize> = if !spec.series.is_empty() {
        spec.series
            .iter()
            .filter_map(|name| find_column(table, name))
            .filter(|index| *index != x)
            .collect()
    } else if spec.kind == ChartKind::Pie {
        Vec::new()
    } else {
        (0..columns.len())
            .filter(|index| *index != x && matches!(type_of(*index), "string" | "bool" | "guid"))
            .collect()
    };
    let mut y_columns: Vec<usize> = if spec.y_columns.is_empty() {
        (0..columns.len())
            .filter(|index| *index != x && !series_columns.contains(index) && is_numeric(type_of(*index)))
            .collect()
    } else {
        spec.y_columns
            .iter()
            .filter_map(|name| find_column(table, name))
            .collect()
    };
    if spec.kind == ChartKind::Pie {
        y_columns.truncate(1);
    }
    if y_columns.is_empty() {
        return Err("The result has no numeric columns to plot.".to_owned());
    }

    let x_type = type_of(x);
    let mut axis = match spec.kind {
        ChartKind::Time | ChartKind::Line | ChartKind::Area | ChartKind::Scatter if x_type == "datetime" => Axis::Time,
        ChartKind::Time | ChartKind::Line | ChartKind::Area | ChartKind::Scatter if is_numeric(x_type) => Axis::Number,
        _ => Axis::Category(Vec::new()),
    };
    let mut notes = Vec::new();
    let mut category_index: HashMap<String, usize> = HashMap::new();
    let mut series_index: HashMap<String, usize> = HashMap::new();
    let mut series: Vec<Series> = Vec::new();
    let mut dropped_series = false;
    let mut dropped_categories = false;
    for row in &table.rows {
        let Some(x_value) = row.get(x) else { continue };
        let x_position = match &mut axis {
            Axis::Time => match x_value.as_str().and_then(parse_datetime_ms) {
                Some(millis) => millis,
                None => continue,
            },
            Axis::Number => match as_number(x_value) {
                Some(number) => number,
                None => continue,
            },
            Axis::Category(labels) => {
                let label = display(x_value, x_type);
                match category_index.get(&label) {
                    Some(index) => *index as f64,
                    None if labels.len() >= MAX_CATEGORIES => {
                        dropped_categories = true;
                        continue;
                    }
                    None => {
                        category_index.insert(label.clone(), labels.len());
                        labels.push(label);
                        (labels.len() - 1) as f64
                    }
                }
            }
        };
        let key = series_columns
            .iter()
            .map(|index| display(row.get(*index).unwrap_or(&serde_json::Value::Null), type_of(*index)))
            .collect::<Vec<_>>()
            .join(" · ");
        for y in &y_columns {
            let Some(value) = row.get(*y).and_then(as_number) else {
                continue;
            };
            let name = match (key.is_empty(), y_columns.len()) {
                (true, _) => columns[*y].name.clone(),
                (false, 1) => key.clone(),
                (false, _) => format!("{key} · {}", columns[*y].name),
            };
            let index = match series_index.get(&name) {
                Some(index) => *index,
                None if series.len() >= MAX_SERIES => {
                    dropped_series = true;
                    continue;
                }
                None => {
                    series_index.insert(name.clone(), series.len());
                    series.push(Series {
                        name,
                        points: Vec::new(),
                    });
                    series.len() - 1
                }
            };
            series[index].points.push((x_position, value));
        }
    }
    if dropped_series {
        notes.push(format!("Showing the first {MAX_SERIES} series."));
    }
    if dropped_categories {
        notes.push(format!("Showing the first {MAX_CATEGORIES} categories."));
    }
    match &mut axis {
        Axis::Category(labels) => {
            for current in &mut series {
                let mut totals = vec![None::<f64>; labels.len()];
                for (position, value) in &current.points {
                    let slot = &mut totals[*position as usize];
                    *slot = Some(slot.unwrap_or(0.0) + value);
                }
                current.points = totals
                    .into_iter()
                    .enumerate()
                    .filter_map(|(index, total)| total.map(|total| (index as f64, total)))
                    .collect();
            }
            if spec.kind == ChartKind::Pie {
                collapse_slices(labels, &mut series, &mut notes);
            }
        }
        Axis::Time | Axis::Number => {
            for current in &mut series {
                current.points.sort_by(|left, right| left.0.total_cmp(&right.0));
            }
        }
    }
    if series.iter().all(|current| current.points.is_empty()) {
        return Err("No rows have plottable values.".to_owned());
    }
    Ok(Model {
        spec: spec.clone(),
        axis,
        x_label: spec.x_title.clone().unwrap_or_else(|| columns[x].name.clone()),
        y_label: spec.y_title.clone().unwrap_or_else(|| {
            if y_columns.len() == 1 {
                columns[y_columns[0]].name.clone()
            } else {
                String::new()
            }
        }),
        series,
        notes,
    })
}

fn collapse_slices(labels: &mut Vec<String>, series: &mut [Series], notes: &mut Vec<String>) {
    let Some(first) = series.first_mut() else { return };
    let mut slices: Vec<(String, f64)> = first
        .points
        .iter()
        .filter(|(_, value)| *value > 0.0)
        .map(|(index, value)| (labels[*index as usize].clone(), *value))
        .collect();
    slices.sort_by(|left, right| right.1.total_cmp(&left.1));
    if slices.len() > MAX_SLICES {
        let other: f64 = slices[MAX_SLICES - 1..].iter().map(|slice| slice.1).sum();
        slices.truncate(MAX_SLICES - 1);
        slices.push(("Other".to_owned(), other));
        notes.push(format!(
            "Smallest slices are combined into 'Other' beyond {MAX_SLICES} slices."
        ));
    }
    *labels = slices.iter().map(|slice| slice.0.clone()).collect();
    first.points = slices
        .iter()
        .enumerate()
        .map(|(index, slice)| (index as f64, slice.1))
        .collect();
}

/// Escapes text for SVG and HTML.
#[must_use]
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

fn truncate_label(label: &str, max: usize) -> String {
    if label.chars().count() <= max {
        label.to_owned()
    } else {
        let mut short: String = label.chars().take(max.saturating_sub(1)).collect();
        short.push('…');
        short
    }
}

/// Formats a value for tooltips.
#[must_use]
pub fn format_value(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        let count = format_count(value.abs() as u64);
        if value < 0.0 { format!("-{count}") } else { count }
    } else {
        let text = format!("{value:.4}");
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    }
}

fn nice_step(range: f64, ticks: f64) -> f64 {
    let raw = (range / ticks.max(1.0)).max(f64::MIN_POSITIVE);
    let magnitude = 10f64.powf(raw.log10().floor());
    let normalized = raw / magnitude;
    let nice = if normalized <= 1.0 {
        1.0
    } else if normalized <= 2.0 {
        2.0
    } else if normalized <= 2.5 {
        2.5
    } else if normalized <= 5.0 {
        5.0
    } else {
        10.0
    };
    nice * magnitude
}

/// A linear or logarithmic value scale.
#[derive(Clone, Copy, Debug)]
struct Scale {
    min: f64,
    max: f64,
    log: bool,
    from: f64,
    to: f64,
}

impl Scale {
    fn map(&self, value: f64) -> f64 {
        let (min, max, value) = if self.log {
            (self.min.log10(), self.max.log10(), value.max(self.min).log10())
        } else {
            (self.min, self.max, value)
        };
        if (max - min).abs() < f64::EPSILON {
            return (self.from + self.to) / 2.0;
        }
        self.from + (value - min) / (max - min) * (self.to - self.from)
    }

    fn ticks(&self) -> Vec<f64> {
        if self.log {
            let mut ticks = Vec::new();
            let mut tick = 10f64.powf(self.min.log10().floor());
            while tick <= self.max * 1.0001 {
                if tick >= self.min * 0.9999 {
                    ticks.push(tick);
                }
                tick *= 10.0;
            }
            return ticks;
        }
        let step = nice_step(self.max - self.min, 5.0);
        let mut ticks = Vec::new();
        let mut tick = (self.min / step).ceil() * step;
        while tick <= self.max + step * 1e-9 && ticks.len() < 50 {
            ticks.push(if tick.abs() < step * 1e-9 { 0.0 } else { tick });
            tick += step;
        }
        ticks
    }
}

fn value_domain(model: &Model) -> (f64, f64) {
    let spec = &model.spec;
    if spec.stacking == Stacking::Stacked100 {
        return (0.0, 100.0);
    }
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    if spec.stacking == Stacking::Stacked && matches!(spec.kind, ChartKind::Column | ChartKind::Bar | ChartKind::Area) {
        let mut positive: HashMap<u64, f64> = HashMap::new();
        let mut negative: HashMap<u64, f64> = HashMap::new();
        for series in &model.series {
            for (x, y) in &series.points {
                let bucket = if *y >= 0.0 { &mut positive } else { &mut negative };
                *bucket.entry(x.to_bits()).or_default() += y;
            }
        }
        for total in positive.values().chain(negative.values()) {
            low = low.min(*total);
            high = high.max(*total);
        }
    } else {
        for (_, y) in model.series.iter().flat_map(|series| series.points.iter()) {
            low = low.min(*y);
            high = high.max(*y);
        }
    }
    if !low.is_finite() || !high.is_finite() {
        return (0.0, 1.0);
    }
    if matches!(spec.kind, ChartKind::Column | ChartKind::Bar | ChartKind::Area) && !spec.log {
        low = low.min(0.0);
        high = high.max(0.0);
    }
    let low = spec.ymin.unwrap_or(low);
    let high = spec.ymax.unwrap_or(high);
    if (high - low).abs() < f64::EPSILON {
        (low - 1.0, high + 1.0)
    } else {
        (low.min(high), high.max(low))
    }
}

fn y_scale(model: &Model, from: f64, to: f64) -> Scale {
    let (low, high) = value_domain(model);
    let positive_low = model
        .series
        .iter()
        .flat_map(|series| series.points.iter().map(|point| point.1))
        .filter(|value| *value > 0.0)
        .fold(f64::INFINITY, f64::min);
    if model.spec.log && positive_low.is_finite() {
        let min = 10f64.powf(positive_low.log10().floor());
        let max = 10f64.powf(high.max(min * 10.0).log10().ceil());
        return Scale {
            min,
            max,
            log: true,
            from,
            to,
        };
    }
    let step = nice_step(high - low, 5.0);
    let min = if model.spec.ymin.is_some() {
        low
    } else {
        (low / step).floor() * step
    };
    let max = if model.spec.ymax.is_some() {
        high
    } else {
        (high / step).ceil() * step
    };
    Scale {
        min,
        max,
        log: false,
        from,
        to,
    }
}

fn x_domain(model: &Model) -> (f64, f64) {
    let mut low = f64::INFINITY;
    let mut high = f64::NEG_INFINITY;
    for (x, _) in model.series.iter().flat_map(|series| series.points.iter()) {
        low = low.min(*x);
        high = high.max(*x);
    }
    if !low.is_finite() {
        return (0.0, 1.0);
    }
    if (high - low).abs() < f64::EPSILON {
        (low - 1.0, high + 1.0)
    } else {
        (low, high)
    }
}

struct Frame {
    left: f64,
    right: f64,
    top: f64,
    bottom: f64,
}

/// Renders `model` as an SVG document sized `width` × `height` pixels.
#[must_use]
pub fn render_svg(model: &Model, width: f64, height: f64) -> String {
    let width = width.max(240.0).floor();
    let height = height.max(160.0).floor();
    let mut svg = String::new();
    let _ = write!(
        svg,
        r#"<svg xmlns="http://www.w3.org/2000/svg" class="kx-chart-svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{}">"#,
        escape(model.spec.title.as_deref().unwrap_or(model.spec.kind.label()))
    );
    let mut top = 12.0;
    if let Some(title) = &model.spec.title {
        let _ = write!(
            svg,
            r#"<text class="kx-chart-title" x="{}" y="18" text-anchor="middle">{}</text>"#,
            width / 2.0,
            escape(title)
        );
        top += 22.0;
    }
    match model.spec.kind {
        ChartKind::Pie => render_pie(&mut svg, model, width, height, top),
        ChartKind::Bar => render_bars(&mut svg, model, width, height, top),
        _ => render_xy(&mut svg, model, width, height, top),
    }
    svg.push_str("</svg>");
    svg
}

fn category_labels(model: &Model) -> &[String] {
    match &model.axis {
        Axis::Category(labels) => labels,
        _ => &[],
    }
}

fn render_y_axis(svg: &mut String, scale: &Scale, frame: &Frame, label: &str, stacked100: bool) {
    svg.push_str(r#"<g class="kx-axis kx-axis-y">"#);
    for tick in scale.ticks() {
        let y = scale.map(tick);
        let text = if stacked100 {
            format!("{}%", format_compact(tick))
        } else {
            format_compact(tick)
        };
        let _ = write!(
            svg,
            r#"<line class="kx-gridline" x1="{:.1}" x2="{:.1}" y1="{y:.1}" y2="{y:.1}"/><text class="kx-tick" x="{:.1}" y="{y:.1}" text-anchor="end" dominant-baseline="middle">{}</text>"#,
            frame.left,
            frame.right,
            frame.left - 6.0,
            escape(&text)
        );
    }
    if !label.is_empty() {
        let middle = (frame.top + frame.bottom) / 2.0;
        let _ = write!(
            svg,
            r#"<text class="kx-axis-title" transform="translate(12 {middle:.1}) rotate(-90)" text-anchor="middle">{}</text>"#,
            escape(&truncate_label(label, 40))
        );
    }
    svg.push_str("</g>");
}

/// Anchors a tick label centered on `x` so that it stays inside a drawing `width` pixels wide.
fn tick_anchor(x: f64, text: &str, width: f64) -> &'static str {
    let half = text.chars().count() as f64 * 3.4;
    if x + half > width - 2.0 {
        "end"
    } else if x - half < 2.0 {
        "start"
    } else {
        "middle"
    }
}

/// Horizontal reach of a category label drawn with `rotate(-40)` and `text-anchor="end"`.
fn rotated_label_reach(label: &str) -> f64 {
    truncate_label(label, 24).chars().count() as f64 * 5.6 * 40f64.to_radians().cos()
}

fn y_label_width(scale: &Scale) -> f64 {
    scale
        .ticks()
        .iter()
        .map(|tick| format_compact(*tick).chars().count())
        .max()
        .unwrap_or(1) as f64
        * 7.0
        + 12.0
}

fn render_xy(svg: &mut String, model: &Model, width: f64, height: f64, top: f64) {
    let spec = &model.spec;
    let labels = category_labels(model);
    let probe = y_scale(model, 0.0, 1.0);
    let mut left = y_label_width(&probe) + if model.y_label.is_empty() { 8.0 } else { 24.0 };
    let right = width - 16.0;
    let longest = labels
        .iter()
        .map(|label| truncate_label(label, 24).chars().count())
        .max()
        .unwrap_or(0) as f64;
    let rotate = !labels.is_empty() && labels.len() as f64 * (longest * 6.5 + 8.0) > right - left;
    if rotate && let Some(first) = labels.first() {
        // Rotated labels extend left of their bar; keep the first one inside the drawing.
        let band = (right - left) / labels.len() as f64;
        left = left.max(rotated_label_reach(first) - band / 2.0 + 6.0).min(right / 2.0);
    }
    let x_title_space = if model.x_label.is_empty() { 0.0 } else { 18.0 };
    let bottom_space = if rotate {
        (longest * 5.2 + 16.0).min(130.0)
    } else {
        26.0
    };
    let frame = Frame {
        left,
        right,
        top,
        bottom: height - bottom_space - x_title_space,
    };
    let scale = y_scale(model, frame.bottom, frame.top);
    render_y_axis(
        svg,
        &scale,
        &frame,
        &model.y_label,
        spec.stacking == Stacking::Stacked100,
    );

    let band = if labels.is_empty() {
        0.0
    } else {
        (frame.right - frame.left) / labels.len() as f64
    };
    let (x_low, x_high) = x_domain(model);
    let x_of = |x: f64| -> f64 {
        if labels.is_empty() {
            frame.left + (x - x_low) / (x_high - x_low) * (frame.right - frame.left)
        } else {
            frame.left + band * (x + 0.5)
        }
    };

    svg.push_str(r#"<g class="kx-axis kx-axis-x">"#);
    let _ = write!(
        svg,
        r#"<line class="kx-axis-line" x1="{:.1}" x2="{:.1}" y1="{:.1}" y2="{:.1}"/>"#,
        frame.left, frame.right, frame.bottom, frame.bottom
    );
    if labels.is_empty() {
        let ticks = ((frame.right - frame.left) / 110.0).max(2.0);
        let (step, time) = match model.axis {
            Axis::Time => (nice_time_step(x_high - x_low, ticks), true),
            _ => (nice_step(x_high - x_low, ticks), false),
        };
        let mut tick = (x_low / step).ceil() * step;
        let mut count = 0;
        while tick <= x_high + step * 1e-9 && count < 60 {
            let x = x_of(tick);
            let text = if time {
                format_timestamp(tick, step)
            } else {
                format_compact(tick)
            };
            let _ = write!(
                svg,
                r#"<text class="kx-tick" x="{x:.1}" y="{:.1}" text-anchor="{}">{}</text>"#,
                frame.bottom + 16.0,
                tick_anchor(x, &text, width),
                escape(&text)
            );
            tick += step;
            count += 1;
        }
    } else {
        let every = if rotate {
            ((labels.len() as f64 * 14.0) / (frame.right - frame.left))
                .ceil()
                .max(1.0) as usize
        } else {
            1
        };
        for (index, label) in labels.iter().enumerate().filter(|(index, _)| index % every == 0) {
            let x = x_of(index as f64);
            let text = escape(&truncate_label(label, 24));
            if rotate {
                let _ = write!(
                    svg,
                    r#"<text class="kx-tick" transform="translate({x:.1} {:.1}) rotate(-40)" text-anchor="end">{text}<title>{}</title></text>"#,
                    frame.bottom + 10.0,
                    escape(label)
                );
            } else {
                let _ = write!(
                    svg,
                    r#"<text class="kx-tick" x="{x:.1}" y="{:.1}" text-anchor="middle">{text}<title>{}</title></text>"#,
                    frame.bottom + 16.0,
                    escape(label)
                );
            }
        }
    }
    if !model.x_label.is_empty() {
        let _ = write!(
            svg,
            r#"<text class="kx-axis-title" x="{:.1}" y="{:.1}" text-anchor="middle">{}</text>"#,
            (frame.left + frame.right) / 2.0,
            height - 6.0,
            escape(&truncate_label(&model.x_label, 60))
        );
    }
    svg.push_str("</g>");

    let describe_x = |x: f64| -> String {
        match &model.axis {
            Axis::Category(labels) => labels.get(x as usize).cloned().unwrap_or_default(),
            Axis::Time => format_timestamp(x, 1.0),
            Axis::Number => format_value(x),
        }
    };
    let totals = column_totals(model);
    svg.push_str(r#"<g class="kx-plot">"#);
    match spec.kind {
        ChartKind::Column => {
            let count = model.series.len().max(1) as f64;
            let inner = band * 0.78;
            let mut stack_positive: HashMap<u64, f64> = HashMap::new();
            let mut stack_negative: HashMap<u64, f64> = HashMap::new();
            for (series_index, series) in model.series.iter().enumerate() {
                for (x, raw) in &series.points {
                    let value = normalize(*raw, *x, &totals, spec.stacking);
                    let (base, end, bar_x, bar_width) = if spec.stacking == Stacking::Grouped {
                        let bar_width = inner / count;
                        (
                            0.0,
                            value,
                            x_of(*x) - inner / 2.0 + bar_width * series_index as f64,
                            bar_width,
                        )
                    } else {
                        let stack = if value >= 0.0 {
                            &mut stack_positive
                        } else {
                            &mut stack_negative
                        };
                        let base = *stack.get(&x.to_bits()).unwrap_or(&0.0);
                        stack.insert(x.to_bits(), base + value);
                        (base, base + value, x_of(*x) - inner / 2.0, inner)
                    };
                    let (y1, y2) = (scale.map(base.max(scale.min)), scale.map(end));
                    let _ = write!(
                        svg,
                        r#"<rect class="kx-bar kx-c{}" x="{bar_x:.2}" y="{:.2}" width="{:.2}" height="{:.2}" rx="1.5"><title>{}</title></rect>"#,
                        series_index % PALETTE_SIZE,
                        y1.min(y2),
                        (bar_width - 1.0).max(1.0),
                        (y1 - y2).abs(),
                        escape(&format!("{} · {}: {}", series.name, describe_x(*x), format_value(*raw)))
                    );
                }
            }
        }
        ChartKind::Line | ChartKind::Time | ChartKind::Area => {
            let mut baseline: HashMap<u64, f64> = HashMap::new();
            for (series_index, series) in model.series.iter().enumerate() {
                let color = series_index % PALETTE_SIZE;
                let stacked = spec.kind == ChartKind::Area && spec.stacking != Stacking::Grouped;
                let mut line = String::new();
                let mut lower = Vec::new();
                for (point_index, (x, raw)) in series.points.iter().enumerate() {
                    let value = normalize(*raw, *x, &totals, spec.stacking);
                    let base = if stacked {
                        *baseline.get(&x.to_bits()).unwrap_or(&0.0)
                    } else {
                        0.0
                    };
                    let top_value = base + value;
                    if stacked {
                        baseline.insert(x.to_bits(), top_value);
                    }
                    let _ = write!(
                        line,
                        "{}{:.2} {:.2}",
                        if point_index == 0 { "M" } else { " L" },
                        x_of(*x),
                        scale.map(top_value)
                    );
                    lower.push((
                        x_of(*x),
                        scale.map(if stacked {
                            base
                        } else {
                            scale.min.max(0.0).min(scale.max)
                        }),
                    ));
                }
                if spec.kind == ChartKind::Area && !lower.is_empty() {
                    let mut area = line.clone();
                    for (x, y) in lower.iter().rev() {
                        let _ = write!(area, " L{x:.2} {y:.2}");
                    }
                    let _ = write!(svg, r#"<path class="kx-area kx-c{color}" d="{area} Z"/>"#);
                }
                let _ = write!(
                    svg,
                    r#"<path class="kx-line kx-c{color}" d="{line}"><title>{}</title></path>"#,
                    escape(&series.name)
                );
                if series.points.len() <= 80 {
                    for (x, raw) in &series.points {
                        let _ = write!(
                            svg,
                            r#"<circle class="kx-point kx-c{color}" cx="{:.2}" cy="{:.2}" r="2.6"><title>{}</title></circle>"#,
                            x_of(*x),
                            scale.map(*raw),
                            escape(&format!("{} · {}: {}", series.name, describe_x(*x), format_value(*raw)))
                        );
                    }
                }
            }
        }
        ChartKind::Scatter => {
            for (series_index, series) in model.series.iter().enumerate() {
                for (x, raw) in &series.points {
                    let _ = write!(
                        svg,
                        r#"<circle class="kx-dot kx-c{}" cx="{:.2}" cy="{:.2}" r="3.5"><title>{}</title></circle>"#,
                        series_index % PALETTE_SIZE,
                        x_of(*x),
                        scale.map(*raw),
                        escape(&format!("{} · {}: {}", series.name, describe_x(*x), format_value(*raw)))
                    );
                }
            }
        }
        ChartKind::Bar | ChartKind::Pie => {}
    }
    svg.push_str("</g>");
}

fn column_totals(model: &Model) -> HashMap<u64, f64> {
    let mut totals: HashMap<u64, f64> = HashMap::new();
    if model.spec.stacking == Stacking::Stacked100 {
        for (x, y) in model.series.iter().flat_map(|series| series.points.iter()) {
            *totals.entry(x.to_bits()).or_default() += y.abs();
        }
    }
    totals
}

fn normalize(value: f64, x: f64, totals: &HashMap<u64, f64>, stacking: Stacking) -> f64 {
    if stacking != Stacking::Stacked100 {
        return value;
    }
    match totals.get(&x.to_bits()) {
        Some(total) if *total > 0.0 => value / total * 100.0,
        _ => 0.0,
    }
}

fn render_bars(svg: &mut String, model: &Model, width: f64, height: f64, top: f64) {
    let spec = &model.spec;
    let labels = category_labels(model);
    let longest = labels
        .iter()
        .map(|label| truncate_label(label, 28).chars().count())
        .max()
        .unwrap_or(4) as f64;
    let frame = Frame {
        left: (longest * 6.6 + 16.0).min(width * 0.4),
        right: width - 20.0,
        top,
        bottom: height - if model.y_label.is_empty() { 26.0 } else { 44.0 },
    };
    let scale = y_scale(model, frame.left, frame.right);
    svg.push_str(r#"<g class="kx-axis kx-axis-x">"#);
    for tick in scale.ticks() {
        let x = scale.map(tick);
        let text = if spec.stacking == Stacking::Stacked100 {
            format!("{}%", format_compact(tick))
        } else {
            format_compact(tick)
        };
        let _ = write!(
            svg,
            r#"<line class="kx-gridline" x1="{x:.1}" x2="{x:.1}" y1="{:.1}" y2="{:.1}"/><text class="kx-tick" x="{x:.1}" y="{:.1}" text-anchor="{}">{}</text>"#,
            frame.top,
            frame.bottom,
            frame.bottom + 16.0,
            tick_anchor(x, &text, width),
            escape(&text)
        );
    }
    if !model.y_label.is_empty() {
        let _ = write!(
            svg,
            r#"<text class="kx-axis-title" x="{:.1}" y="{:.1}" text-anchor="middle">{}</text>"#,
            (frame.left + frame.right) / 2.0,
            height - 6.0,
            escape(&truncate_label(&model.y_label, 60))
        );
    }
    svg.push_str("</g>");
    let band = if labels.is_empty() {
        0.0
    } else {
        (frame.bottom - frame.top) / labels.len() as f64
    };
    let every = ((14.0 / band.max(0.1)).ceil() as usize).max(1);
    svg.push_str(r#"<g class="kx-axis kx-axis-y">"#);
    for (index, label) in labels.iter().enumerate().filter(|(index, _)| index % every == 0) {
        let _ = write!(
            svg,
            r#"<text class="kx-tick" x="{:.1}" y="{:.1}" text-anchor="end" dominant-baseline="middle">{}<title>{}</title></text>"#,
            frame.left - 6.0,
            frame.top + band * (index as f64 + 0.5),
            escape(&truncate_label(label, 28)),
            escape(label)
        );
    }
    svg.push_str("</g>");
    let totals = column_totals(model);
    let count = model.series.len().max(1) as f64;
    let inner = band * 0.78;
    let mut stacks: HashMap<(u64, bool), f64> = HashMap::new();
    svg.push_str(r#"<g class="kx-plot">"#);
    for (series_index, series) in model.series.iter().enumerate() {
        for (x, raw) in &series.points {
            let value = normalize(*raw, *x, &totals, spec.stacking);
            let center = frame.top + band * (x + 0.5);
            let (base, end, bar_y, bar_height) = if spec.stacking == Stacking::Grouped {
                let bar_height = inner / count;
                (
                    0.0,
                    value,
                    center - inner / 2.0 + bar_height * series_index as f64,
                    bar_height,
                )
            } else {
                let key = (x.to_bits(), value >= 0.0);
                let base = *stacks.get(&key).unwrap_or(&0.0);
                stacks.insert(key, base + value);
                (base, base + value, center - inner / 2.0, inner)
            };
            let (x1, x2) = (scale.map(base.max(scale.min)), scale.map(end));
            let label = labels.get(*x as usize).cloned().unwrap_or_default();
            let _ = write!(
                svg,
                r#"<rect class="kx-bar kx-c{}" x="{:.2}" y="{bar_y:.2}" width="{:.2}" height="{:.2}" rx="1.5"><title>{}</title></rect>"#,
                series_index % PALETTE_SIZE,
                x1.min(x2),
                (x1 - x2).abs(),
                (bar_height - 1.0).max(1.0),
                escape(&format!("{} · {label}: {}", series.name, format_value(*raw)))
            );
        }
    }
    svg.push_str("</g>");
}

fn render_pie(svg: &mut String, model: &Model, width: f64, height: f64, top: f64) {
    let labels = category_labels(model);
    let Some(series) = model.series.first() else { return };
    let total: f64 = series.points.iter().map(|point| point.1.max(0.0)).sum();
    if total <= 0.0 {
        return;
    }
    let center_x = width / 2.0;
    let center_y = top + (height - top) / 2.0;
    let radius = ((width.min(height - top)) / 2.0 - 12.0).max(20.0);
    let inner = radius * 0.56;
    let mut angle = -std::f64::consts::FRAC_PI_2;
    svg.push_str(r#"<g class="kx-plot">"#);
    for (index, (_, value)) in series.points.iter().enumerate() {
        let sweep = value.max(0.0) / total * std::f64::consts::TAU;
        let label = labels.get(index).cloned().unwrap_or_default();
        let title = escape(&format!(
            "{label}: {} ({:.1}%)",
            format_value(*value),
            value / total * 100.0
        ));
        let color = index % PALETTE_SIZE;
        if sweep >= std::f64::consts::TAU - 1e-9 {
            let _ = write!(
                svg,
                r#"<path class="kx-slice kx-c{color}" fill-rule="evenodd" d="M{:.2} {center_y:.2}a{radius:.2} {radius:.2} 0 1 0 {:.2} 0a{radius:.2} {radius:.2} 0 1 0 {:.2} 0ZM{:.2} {center_y:.2}a{inner:.2} {inner:.2} 0 1 0 {:.2} 0a{inner:.2} {inner:.2} 0 1 0 {:.2} 0Z"><title>{title}</title></path>"#,
                center_x - radius,
                radius * 2.0,
                -radius * 2.0,
                center_x - inner,
                inner * 2.0,
                -inner * 2.0
            );
        } else {
            let end = angle + sweep;
            let large = i32::from(sweep > std::f64::consts::PI);
            let point = |r: f64, a: f64| (center_x + r * a.cos(), center_y + r * a.sin());
            let (outer_start, outer_end) = (point(radius, angle), point(radius, end));
            let (inner_end, inner_start) = (point(inner, end), point(inner, angle));
            let _ = write!(
                svg,
                r#"<path class="kx-slice kx-c{color}" d="M{:.2} {:.2}A{radius:.2} {radius:.2} 0 {large} 1 {:.2} {:.2}L{:.2} {:.2}A{inner:.2} {inner:.2} 0 {large} 0 {:.2} {:.2}Z"><title>{title}</title></path>"#,
                outer_start.0,
                outer_start.1,
                outer_end.0,
                outer_end.1,
                inner_end.0,
                inner_end.1,
                inner_start.0,
                inner_start.1
            );
        }
        angle += sweep;
    }
    let _ = write!(
        svg,
        r#"<text class="kx-pie-total" x="{center_x:.1}" y="{:.1}" text-anchor="middle">{}</text><text class="kx-tick" x="{center_x:.1}" y="{:.1}" text-anchor="middle">{}</text>"#,
        center_y + 2.0,
        escape(&format_compact(total)),
        center_y + 20.0,
        escape(&truncate_label(&model.y_label, 24))
    );
    svg.push_str("</g>");
}

#[cfg(test)]
mod tests {
    use super::*;
    use kusto_explorer_api::ResultColumn;
    use serde_json::json;

    fn column(name: &str, column_type: &str) -> ResultColumn {
        ResultColumn {
            name: name.to_owned(),
            column_type: column_type.to_owned(),
        }
    }

    fn counts() -> ResultTable {
        ResultTable {
            name: "PrimaryResult".to_owned(),
            columns: vec![column("TableName", "string"), column("Rows", "long")],
            rows: (0..26)
                .map(|index| vec![json!(format!("t{index:02}")), json!(index * 10)])
                .collect(),
            total_rows: 26,
            truncated: false,
        }
    }

    fn count_matches(text: &str, needle: &str) -> usize {
        text.matches(needle).count()
    }

    #[test]
    fn tick_labels_near_the_edges_stay_inside_the_drawing() {
        assert_eq!(tick_anchor(400.0, "12:02:00", 800.0), "middle");
        assert_eq!(tick_anchor(790.0, "12:02:00", 800.0), "end");
        assert_eq!(tick_anchor(10.0, "12:02:00", 800.0), "start");
    }

    #[test]
    fn rotated_category_labels_start_inside_the_drawing() {
        let mut table = counts();
        for (index, row) in table.rows.iter_mut().enumerate() {
            row[0] = json!(format!("dataflow_sqlserver_availability_group_states_{index:02}"));
        }
        let model = build(&table, &Spec::new(ChartKind::Column)).unwrap_or_else(|_| unreachable!());
        let svg = render_svg(&model, 1240.0, 330.0);
        let rotated = svg
            .split(r#"<text class="kx-tick" transform="translate("#)
            .nth(1)
            .unwrap_or_default();
        let x: f64 = rotated
            .split(' ')
            .next()
            .and_then(|x| x.parse().ok())
            .unwrap_or_default();
        assert!(x > 0.0, "a rotated label was drawn");
        let first = category_labels(&model).first().cloned().unwrap_or_default();
        assert!(x - rotated_label_reach(&first) >= 0.0, "first label at {x} is clipped");
    }

    #[test]
    fn column_chart_draws_one_bar_per_category() {
        let visualization = Visualization {
            visualization: "columnchart".to_owned(),
            ..Visualization::default()
        };
        let spec = Spec::from_visualization(&visualization, None).unwrap_or_else(|| Spec::new(ChartKind::Pie));
        assert_eq!(spec.kind, ChartKind::Column);
        let model = build(&counts(), &spec).unwrap_or_else(|_| unreachable!());
        assert_eq!(model.series.len(), 1);
        assert_eq!(model.series[0].name, "Rows");
        assert!(!model.show_legend());
        let svg = render_svg(&model, 800.0, 400.0);
        assert_eq!(count_matches(&svg, r#"class="kx-bar "#), 26);
        assert!(svg.contains("<title>Rows · t25: 250</title>"));
        assert!(svg.starts_with("<svg") && svg.ends_with("</svg>"));
    }

    #[test]
    fn time_charts_split_series_by_string_columns() {
        let table = ResultTable {
            name: "PrimaryResult".to_owned(),
            columns: vec![
                column("Timestamp", "datetime"),
                column("Host", "string"),
                column("Count", "long"),
            ],
            rows: vec![
                vec![json!("2026-01-01T00:01:00Z"), json!("b"), json!(2)],
                vec![json!("2026-01-01T00:00:00Z"), json!("a"), json!(1)],
                vec![json!("2026-01-01T00:00:00Z"), json!("b"), json!(3)],
                vec![json!("bad"), json!("a"), json!(9)],
            ],
            total_rows: 4,
            truncated: false,
        };
        let model = build(&table, &Spec::new(ChartKind::Time)).unwrap_or_else(|_| unreachable!());
        assert_eq!(model.axis, Axis::Time);
        let names: Vec<&str> = model.series.iter().map(|series| series.name.as_str()).collect();
        assert_eq!(names, ["b", "a"]);
        assert!(model.series[0].points[0].0 < model.series[0].points[1].0);
        assert!(model.show_legend());
        let svg = render_svg(&model, 640.0, 320.0);
        assert_eq!(count_matches(&svg, r#"class="kx-line "#), 2);
        assert_eq!(count_matches(&svg, r#"class="kx-point "#), 3);
    }

    #[test]
    fn explicit_columns_and_errors() {
        let mut spec = Spec::new(ChartKind::Column);
        spec.x_column = Some("missing".to_owned());
        assert!(build(&counts(), &spec).is_err());
        let text_only = ResultTable {
            columns: vec![column("Name", "string")],
            rows: vec![vec![json!("x")]],
            ..counts()
        };
        assert_eq!(
            build(&text_only, &Spec::new(ChartKind::Column)),
            Err("The result has no numeric columns to plot.".to_owned())
        );
    }

    #[test]
    fn duplicate_categories_are_summed_and_pies_collapse() {
        let table = ResultTable {
            rows: (0..20)
                .map(|index| vec![json!(format!("k{}", index % 15)), json!(1)])
                .collect(),
            ..counts()
        };
        let pie = build(&table, &Spec::new(ChartKind::Pie)).unwrap_or_else(|_| unreachable!());
        assert_eq!(pie.series[0].points.len(), MAX_SLICES);
        assert_eq!(pie.series[0].points[0].1, 2.0);
        let svg = render_svg(&pie, 500.0, 400.0);
        assert_eq!(count_matches(&svg, r#"class="kx-slice "#), MAX_SLICES);
        assert_eq!(pie.legend().len(), MAX_SLICES);
        assert!(!pie.notes.is_empty());
    }

    #[test]
    fn stacked_and_bar_charts_render() {
        let table = ResultTable {
            columns: vec![
                column("Day", "string"),
                column("Kind", "string"),
                column("Count", "long"),
            ],
            rows: vec![
                vec![json!("mon"), json!("a"), json!(1)],
                vec![json!("mon"), json!("b"), json!(3)],
                vec![json!("tue"), json!("a"), json!(2)],
            ],
            ..counts()
        };
        let visualization = Visualization {
            visualization: "barchart".to_owned(),
            kind: Some("stacked100".to_owned()),
            ..Visualization::default()
        };
        let spec = Spec::from_visualization(&visualization, None).unwrap_or_else(|| Spec::new(ChartKind::Pie));
        assert_eq!(spec.stacking, Stacking::Stacked100);
        let model = build(&table, &spec).unwrap_or_else(|_| unreachable!());
        let svg = render_svg(&model, 600.0, 300.0);
        assert_eq!(count_matches(&svg, r#"class="kx-bar "#), 3);
        assert!(svg.contains("100%"));
        let column = render_svg(
            &build(
                &table,
                &Spec {
                    kind: ChartKind::Column,
                    ..spec
                },
            )
            .unwrap_or_else(|_| unreachable!()),
            600.0,
            300.0,
        );
        assert_eq!(count_matches(&column, r#"class="kx-bar "#), 3);
    }

    #[test]
    fn helpers_escape_and_format() {
        assert_eq!(escape("<a & 'b'>"), "&lt;a &amp; &#39;b&#39;&gt;");
        assert_eq!(format_value(1_234.0), "1,234");
        assert_eq!(format_value(-5.0), "-5");
        assert_eq!(format_value(0.125), "0.125");
        assert_eq!(nice_step(97.0, 5.0), 20.0);
        assert_eq!(ChartKind::from_id("pie"), Some(ChartKind::Pie));
        assert_eq!(ChartKind::from_render("TimeChart"), Some(ChartKind::Time));
        assert_eq!(ChartKind::from_render("card"), None);
    }
}
