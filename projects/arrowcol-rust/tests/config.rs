use arrowcol_rust::{CollectorConfig, ConfigError};

#[test]
fn checked_in_configs_are_strict_and_valid() -> Result<(), ConfigError> {
    CollectorConfig::from_yaml(include_bytes!("../config.yaml"))?;
    CollectorConfig::from_yaml(include_bytes!("../config.docker.yaml"))?;
    Ok(())
}

#[test]
fn unknown_fields_are_rejected() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?
        .replace("  bind: 127.0.0.1:50051", "  bind: 127.0.0.1:50051\n  surprise: true");
    let Err(error) = CollectorConfig::from_yaml(yaml.as_bytes()) else {
        anyhow::bail!("unknown field was accepted");
    };
    assert!(matches!(error, ConfigError::Yaml(_)));
    Ok(())
}

#[test]
fn native_quiver_is_rejected_with_typed_capability_error() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?
        .replace(
            "  native_quiver:\n    enabled: false\n    directory: null\n    capacity_bytes: null",
            "  native_quiver:\n    enabled: true\n    directory: projects/arrowcol-rust/.data/quiver\n    capacity_bytes: 1048576",
        );
    let Err(error) = CollectorConfig::from_yaml(yaml.as_bytes()) else {
        anyhow::bail!("unsupported Quiver configuration was accepted");
    };
    assert!(matches!(
        error,
        ConfigError::UnsupportedCapability {
            capability: "native_quiver_generic_arrow",
            ..
        }
    ));
    Ok(())
}

#[test]
fn unsafe_lease_timing_is_rejected() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?
        .replace("  table_lease_renew_ms: 5000", "  table_lease_renew_ms: 20000");
    let Err(error) = CollectorConfig::from_yaml(yaml.as_bytes()) else {
        anyhow::bail!("unsafe renewal timing was accepted");
    };
    assert!(error.to_string().contains("less than half table_lease_ttl_ms"));
    Ok(())
}

#[test]
fn sql_pool_defaults_preserve_existing_configuration() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?.replace(
        "  pool:\n    receipts_max_size: 16\n    routing_max_size: 4\n    catalog_max_size: 4\n    acquire_timeout_ms: 5000\n",
        "",
    );
    let config = CollectorConfig::from_yaml(yaml.as_bytes())?;
    assert_eq!(config.sql_server.pool.receipts_max_size, 16);
    assert_eq!(config.sql_server.pool.routing_max_size, 4);
    assert_eq!(config.sql_server.pool.catalog_max_size, 4);
    assert_eq!(config.sql_server.pool.acquire_timeout_ms, 5000);
    Ok(())
}

#[test]
fn invalid_sql_pool_settings_are_rejected() -> anyhow::Result<()> {
    let yaml = String::from_utf8(include_bytes!("../config.yaml").to_vec())?;
    for setting in [
        "receipts_max_size: 16",
        "routing_max_size: 4",
        "catalog_max_size: 4",
        "acquire_timeout_ms: 5000",
    ] {
        let key = setting
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("missing setting value"))?
            .0;
        let invalid_yaml = yaml.replace(setting, &format!("{key}: 0"));
        let error = CollectorConfig::from_yaml(invalid_yaml.as_bytes())
            .err()
            .ok_or_else(|| anyhow::anyhow!("zero {key} was accepted"))?;
        assert!(matches!(error, ConfigError::Invalid(_)));
        assert!(error.to_string().contains(key));
    }
    let unknown = yaml.replace(
        "    routing_max_size: 4",
        "    routing_max_size: 4\n    unknown_pool_setting: 1",
    );
    assert!(matches!(
        CollectorConfig::from_yaml(unknown.as_bytes()),
        Err(ConfigError::Yaml(_))
    ));
    let oversized = yaml.replace("receipts_max_size: 16", &format!("receipts_max_size: {}", usize::MAX));
    assert!(matches!(
        CollectorConfig::from_yaml(oversized.as_bytes()),
        Err(ConfigError::Invalid(_))
    ));
    Ok(())
}
