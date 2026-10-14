use std::time::Duration;

use arrowcol_rust::metrics::CollectorMetrics;
use arrowcol_sql_server::{ConnectionConfig, PoolConfig, SqlPool};

#[test]
fn sql_pool_metrics_are_role_labeled_and_do_not_open_connections() -> anyhow::Result<()> {
    let metrics = CollectorMetrics::new()?;
    let pools = [("receipts", 16), ("routing", 4), ("catalog", 4)]
        .into_iter()
        .map(|(role, max_size)| {
            SqlPool::new(
                ConnectionConfig::new("not-contacted.invalid", 1433, "database", "username", "secret"),
                PoolConfig {
                    max_size,
                    acquire_timeout: Duration::from_secs(5),
                },
            )
            .map(|pool| (role, pool))
        })
        .collect::<Result<Vec<_>, _>>()?;
    metrics.register_sql_pools(pools)?;
    let output = String::from_utf8(metrics.encode()?)?;
    assert!(output.contains("# TYPE arrowcol_direct_sql_pool_created_total counter"));
    assert!(output.contains("# TYPE arrowcol_direct_sql_pool_size gauge"));
    for (role, max_size) in [("receipts", 16), ("routing", 4), ("catalog", 4)] {
        assert!(output.contains(&format!("arrowcol_direct_sql_pool_created_total{{role=\"{role}\"}} 0")));
        assert!(output.contains(&format!(
            "arrowcol_direct_sql_pool_max_size{{role=\"{role}\"}} {max_size}"
        )));
    }
    assert!(!output.contains("secret"));
    assert!(!output.contains("username"));
    assert!(!output.contains("not-contacted.invalid"));
    Ok(())
}
