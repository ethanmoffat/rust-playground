//! Starter queries shown in the side bar.

/// A ready-to-run query.
#[derive(Clone, Copy, Debug)]
pub struct Starter {
    /// Stable identifier.
    pub id: &'static str,
    /// Group heading.
    pub group: &'static str,
    /// Title.
    pub title: &'static str,
    /// One-line description.
    pub description: &'static str,
    /// Codicon name.
    pub icon: &'static str,
    /// Query text.
    pub query: &'static str,
}

/// Starter queries in display order.
pub const STARTERS: &[Starter] = &[
    Starter {
        id: "row-counts",
        group: "Explore",
        title: "Row count per table",
        description: "Counts every row in every table of the database.",
        icon: "table",
        query: "union withsource=TableName *\n| summarize Rows = count() by TableName\n| order by TableName asc",
    },
    Starter {
        id: "row-counts-chart",
        group: "Explore",
        title: "Rows per table chart",
        description: "The same counts drawn as a column chart.",
        icon: "graph",
        query: "union withsource=TableName *\n| summarize Rows = count() by TableName\n| order by Rows desc\n| render columnchart with (title = \"Rows per table\")",
    },
    Starter {
        id: "row-share",
        group: "Explore",
        title: "Share of rows",
        description: "Which tables hold most of the data.",
        icon: "pie-chart",
        query: "union withsource=TableName *\n| summarize Rows = count() by TableName\n| order by Rows desc\n| render piechart",
    },
    Starter {
        id: "ingestion-timeline",
        group: "Ingestion",
        title: "Ingestion timeline",
        description: "Rows ingested per minute across all tables.",
        icon: "graph-line",
        query: "union *\n| summarize Rows = count() by bin(ingestion_time(), 1m)\n| render timechart",
    },
    Starter {
        id: "latest-ingestion",
        group: "Ingestion",
        title: "Latest ingestion per table",
        description: "When each table last received data.",
        icon: "watch",
        query: "union withsource=TableName *\n| summarize LastIngested = max(ingestion_time()) by TableName\n| order by LastIngested desc",
    },
    Starter {
        id: "ingestion-failures",
        group: "Ingestion",
        title: "Ingestion failures",
        description: "Failed ingestion operations reported by the cluster.",
        icon: "warning",
        query: ".show ingestion failures",
    },
    Starter {
        id: "table-details",
        group: "Storage",
        title: "Table sizes",
        description: "Rows, extents, and original size per table.",
        icon: "database",
        query: ".show tables details\n| project TableName, TotalRowCount, TotalExtents, TotalOriginalSize\n| order by TableName asc",
    },
    Starter {
        id: "extents",
        group: "Storage",
        title: "Extents per table",
        description: "Data shards with their compressed and original sizes.",
        icon: "layers",
        query: ".show extents\n| summarize Extents = count(), Rows = sum(RowCount), OriginalSize = sum(OriginalSize), CompressedSize = sum(ExtentSize) by TableName\n| order by TableName asc",
    },
    Starter {
        id: "schema",
        group: "Storage",
        title: "Columns and types",
        description: "Every column of every table.",
        icon: "symbol-field",
        query: ".show database schema\n| where isnotempty(ColumnName)\n| project TableName, ColumnName, ColumnType",
    },
    Starter {
        id: "recent-commands",
        group: "Operations",
        title: "Recent commands",
        description: "The latest management commands and their state.",
        icon: "terminal",
        query: ".show commands\n| top 50 by StartedOn desc\n| project StartedOn, CommandType, State, Duration, Text",
    },
    Starter {
        id: "recent-queries",
        group: "Operations",
        title: "Recent queries",
        description: "The latest queries and their duration.",
        icon: "history",
        query: ".show queries\n| top 50 by StartedOn desc\n| project StartedOn, State, Duration, Text",
    },
    Starter {
        id: "cluster",
        group: "Operations",
        title: "Cluster and version",
        description: "Nodes and the service build.",
        icon: "server",
        query: ".show cluster\n\n.show version",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starters_are_unique_and_non_empty() {
        let ids: std::collections::HashSet<&str> = STARTERS.iter().map(|starter| starter.id).collect();
        assert_eq!(ids.len(), STARTERS.len());
        assert!(
            STARTERS
                .iter()
                .all(|starter| !starter.query.trim().is_empty() && !starter.title.is_empty())
        );
    }
}
