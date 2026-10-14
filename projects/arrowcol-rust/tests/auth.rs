use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use arrowcol_flight::TrustedIdentityValidator;
use arrowcol_rust::auth::{
    INTERNAL_AUTH_CONTEXT_HEADER, INTERNAL_AUTH_EXPIRES_HEADER, INTERNAL_AUTH_SIGNATURE_HEADER,
    RouterIdentityValidator, allow_all_context,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::metadata::{MetadataMap, MetadataValue};

#[tokio::test]
async fn router_identity_signature_is_verified_and_tampering_fails() -> Result<()> {
    let secret = b"0123456789abcdef0123456789abcdef".to_vec();
    let validator = RouterIdentityValidator::new(secret.clone(), 30_000)?;
    let context = allow_all_context("router-test")?;
    let encoded = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&context)?);
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let expiry = now + 10_000;
    let canonical = format!("{encoded}\n{expiry}");
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret)?;
    mac.update(canonical.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    let mut metadata = MetadataMap::new();
    metadata.insert(INTERNAL_AUTH_CONTEXT_HEADER, MetadataValue::try_from(encoded.as_str())?);
    metadata.insert(
        INTERNAL_AUTH_EXPIRES_HEADER,
        MetadataValue::try_from(expiry.to_string())?,
    );
    metadata.insert(
        INTERNAL_AUTH_SIGNATURE_HEADER,
        MetadataValue::try_from(signature.as_str())?,
    );

    let validated = validator
        .validate(&metadata)
        .await?
        .context("trusted identity was not returned")?;
    assert_eq!(validated.principal().subject().as_str(), "router-test");

    metadata.insert(
        INTERNAL_AUTH_SIGNATURE_HEADER,
        MetadataValue::try_from("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")?,
    );
    assert!(validator.validate(&metadata).await.is_err());
    Ok(())
}
