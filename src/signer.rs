//! The signing key for live mode.
//!
//! `HYPERKEEPER_PRIVATE_KEY` (hex) takes precedence. Otherwise the configured keystore is
//! decrypted with `HYPERKEEPER_KEYSTORE_PASSWORD`. The key is never logged; only its address.

use anyhow::{Context, bail};
use hypersdk::hypercore::PrivateKeySigner;

use crate::config::SignerConfig;

pub const PRIVATE_KEY_ENV: &str = "HYPERKEEPER_PRIVATE_KEY";
pub const PASSWORD_ENV: &str = "HYPERKEEPER_KEYSTORE_PASSWORD";

/// Loads the key, or a throwaway one when `throwaway_ok` and none is configured.
pub fn load(config: &SignerConfig, throwaway_ok: bool) -> anyhow::Result<(PrivateKeySigner, bool)> {
    if let Ok(hex) = std::env::var(PRIVATE_KEY_ENV) {
        let signer: PrivateKeySigner = hex
            .trim()
            .parse()
            .with_context(|| format!("{PRIVATE_KEY_ENV} is not a valid private key"))?;
        return Ok((signer, false));
    }
    if let Some(path) = &config.keystore {
        let password = std::env::var(PASSWORD_ENV)
            .with_context(|| format!("signer.keystore is set but {PASSWORD_ENV} is not"))?;
        let signer = PrivateKeySigner::decrypt_keystore(path, password)
            .with_context(|| format!("decrypting {}", path.display()))?;
        return Ok((signer, false));
    }
    if throwaway_ok {
        return Ok((PrivateKeySigner::random(), true));
    }
    bail!("no signing key: set {PRIVATE_KEY_ENV}, or signer.keystore and {PASSWORD_ENV}")
}
