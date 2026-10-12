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
