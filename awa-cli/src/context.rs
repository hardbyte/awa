//! Named CLI contexts (#437).
//!
//! A context names a database target in `~/.config/awa/config.toml` so
//! operators working against several Awa databases (e.g. two production
//! Postgres instances) never rely on whatever `DATABASE_URL` happens to be
//! exported in the current shell. Deliberately kubectl-*unlike* in one way:
//! there is no sticky "use-context" for mutations — once more than one
//! context is defined, mutating commands must name their target explicitly.
//!
//! ```toml
//! default_context = "cloudsql"   # honored by read-only commands only
//!
//! [contexts.cloudsql]
//! url_env = "AWA_CLOUDSQL_URL"   # indirection: no secret in the file
//! production = true
//!
//! [contexts.alloydb]
//! url_env = "AWA_ALLOYDB_URL"
//! production = true
//! ```
//!
//! Resolution order: `--database-url` > `--context`/`AWA_CONTEXT` >
//! `DATABASE_URL` env > `default_context`. The safety rules carry the
//! weight, not the order:
//!
//! 1. Read-only commands may fall back to `DATABASE_URL` or
//!    `default_context`. Mutating commands require an explicit `--context`
//!    or `--database-url` whenever more than one context is defined.
//! 2. The resolved target is echoed (password-stripped) to stderr before
//!    any command acts.
//! 3. `production = true` gates mutating commands behind an interactive
//!    y/N confirmation (`--yes` skips it for automation).
//!
//! Everything here is pure and unit-testable except the thin filesystem
//! wrapper [`load_from_default_path`].

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

/// Parsed `~/.config/awa/config.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    /// Context used when nothing more explicit is given. Honored by
    /// read-only commands only (see [`Access`]).
    pub default_context: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, ContextEntry>,
}

/// One `[contexts.<name>]` table. Exactly one of `url` / `url_env` must be
/// set; `url_env` keeps secrets out of the file (the variable is read at
/// resolution time).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextEntry {
    pub url: Option<String>,
    pub url_env: Option<String>,
    /// Marks a production database: mutating commands prompt for
    /// confirmation before acting (skippable with `--yes`).
    #[serde(default)]
    pub production: bool,
}

/// Whether a command only reads queue state or can change it. Mutating
/// commands are held to the stricter explicit-target rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    ReadOnly,
    Mutating,
}

/// How the resolved database target was selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetSource {
    /// `--database-url` on the command line.
    CliFlag,
    /// `--context <name>` or `AWA_CONTEXT`.
    NamedContext,
    /// The `DATABASE_URL` environment variable.
    EnvDatabaseUrl,
    /// `default_context` from the config file.
    DefaultContext,
}

/// The database target a command will act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTarget {
    pub url: String,
    /// Context name when the target came from the config file.
    pub context: Option<String>,
    /// True when the selected context is marked `production = true`.
    pub production: bool,
    pub source: TargetSource,
}

impl ResolvedTarget {
    /// One-line, password-stripped description for the pre-action echo.
    pub fn describe(&self) -> String {
        let location = redact_url(&self.url);
        let production = if self.production { " [production]" } else { "" };
        match (self.source, self.context.as_deref()) {
            (TargetSource::CliFlag, _) => format!("target: {location} (--database-url)"),
            (TargetSource::EnvDatabaseUrl, _) => format!("target: {location} (DATABASE_URL env)"),
            (TargetSource::NamedContext, Some(name)) => {
                format!("target: context '{name}'{production} — {location}")
            }
            (TargetSource::DefaultContext, Some(name)) => {
                format!("target: default context '{name}'{production} — {location}")
            }
            // Context sources always carry a name; unreachable in practice.
            (_, None) => format!("target: {location}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("failed to parse awa config: {0}")]
    Parse(String),
    #[error("context '{name}' is invalid: {reason}")]
    InvalidContext { name: String, reason: String },
    #[error("default_context '{0}' does not name a defined [contexts.*] table")]
    UnknownDefaultContext(String),
    #[error("unknown context '{0}' — run `awa context list` to see defined contexts")]
    UnknownContext(String),
    #[error(
        "context '{context}' uses url_env = \"{var}\" but the environment variable is not set"
    )]
    UrlEnvUnset { context: String, var: String },
    #[error(
        "multiple contexts are defined and this command mutates the database — pass an \
         explicit --context <name> or --database-url (DATABASE_URL and default_context are \
         honored only for read-only commands when more than one context exists)"
    )]
    ExplicitContextRequired,
    #[error(
        "no database target: set --database-url, --context <name> (with \
         ~/.config/awa/config.toml), DATABASE_URL, or a default_context"
    )]
    NoDatabaseUrl,
}

/// Parse and validate a config file's contents.
pub fn parse_config(text: &str) -> Result<ConfigFile, ContextError> {
    let config: ConfigFile =
        toml::from_str(text).map_err(|err| ContextError::Parse(err.to_string()))?;
    for (name, entry) in &config.contexts {
        match (&entry.url, &entry.url_env) {
            (Some(_), Some(_)) => {
                return Err(ContextError::InvalidContext {
                    name: name.clone(),
                    reason: "set exactly one of `url` or `url_env`, not both".into(),
                });
            }
            (None, None) => {
                return Err(ContextError::InvalidContext {
                    name: name.clone(),
                    reason: "set one of `url` or `url_env`".into(),
                });
            }
            _ => {}
        }
    }
    if let Some(default) = &config.default_context {
        if !config.contexts.contains_key(default) {
            return Err(ContextError::UnknownDefaultContext(default.clone()));
        }
    }
    Ok(config)
}

/// Resolve the database target for a command.
///
/// `env_lookup` abstracts `std::env::var` so resolution stays pure and
/// unit-testable; it is used for `url_env` indirection only (the
/// `DATABASE_URL` and `AWA_CONTEXT` variables are read by the caller and
/// passed in explicitly).
pub fn resolve_target<F>(
    cli_database_url: Option<&str>,
    context_name: Option<&str>,
    env_database_url: Option<&str>,
    config: Option<&ConfigFile>,
    access: Access,
    env_lookup: F,
) -> Result<ResolvedTarget, ContextError>
where
    F: Fn(&str) -> Option<String>,
{
    // 1. Explicit --database-url always wins and is never gated: the
    //    operator has already named the exact target.
    if let Some(url) = cli_database_url {
        return Ok(ResolvedTarget {
            url: url.to_string(),
            context: None,
            production: false,
            source: TargetSource::CliFlag,
        });
    }

    // 2. Explicit --context / AWA_CONTEXT.
    if let Some(name) = context_name {
        let config = config.ok_or_else(|| ContextError::UnknownContext(name.to_string()))?;
        let entry = config
            .contexts
            .get(name)
            .ok_or_else(|| ContextError::UnknownContext(name.to_string()))?;
        let url = context_url(name, entry, &env_lookup)?;
        return Ok(ResolvedTarget {
            url,
            context: Some(name.to_string()),
            production: entry.production,
            source: TargetSource::NamedContext,
        });
    }

    let multiple_contexts = config.is_some_and(|c| c.contexts.len() > 1);

    // 3. DATABASE_URL env — a shell-level default, which is exactly the
    //    wrong-database foot-gun once several contexts exist. Read-only
    //    commands may use it; mutations must be explicit.
    if let Some(url) = env_database_url {
        if access == Access::Mutating && multiple_contexts {
            return Err(ContextError::ExplicitContextRequired);
        }
        return Ok(ResolvedTarget {
            url: url.to_string(),
            context: None,
            production: false,
            source: TargetSource::EnvDatabaseUrl,
        });
    }

    // 4. default_context — same asymmetry as DATABASE_URL.
    if let Some(config) = config {
        if let Some(name) = &config.default_context {
            if access == Access::Mutating && multiple_contexts {
                return Err(ContextError::ExplicitContextRequired);
            }
            let entry = config
                .contexts
                .get(name)
                .ok_or_else(|| ContextError::UnknownDefaultContext(name.clone()))?;
            let url = context_url(name, entry, &env_lookup)?;
            return Ok(ResolvedTarget {
                url,
                context: Some(name.clone()),
                production: entry.production,
                source: TargetSource::DefaultContext,
            });
        }
    }

    Err(ContextError::NoDatabaseUrl)
}

fn context_url<F>(name: &str, entry: &ContextEntry, env_lookup: &F) -> Result<String, ContextError>
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(url) = &entry.url {
        return Ok(url.clone());
    }
    // parse_config guarantees exactly one of url / url_env is set.
    let var = entry.url_env.as_deref().unwrap_or_default();
    env_lookup(var).ok_or_else(|| ContextError::UrlEnvUnset {
        context: name.to_string(),
        var: var.to_string(),
    })
}

/// Strip credentials from a database URL for display. Redacts the userinfo
/// password and any `password=` query parameter (libpq-style). URLs that do
/// not parse are not echoed at all — they could embed credentials anywhere.
pub fn redact_url(raw: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw) else {
        return "<unparseable database URL, not shown>".to_string();
    };
    if url.password().is_some() {
        // Setting a password can only fail for non-special schemes without
        // a host; postgres URLs always have one.
        let _ = url.set_password(Some("REDACTED"));
    }
    if url.query().is_some() {
        let redacted: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                if k.eq_ignore_ascii_case("password") {
                    (k.into_owned(), "REDACTED".to_string())
                } else {
                    (k.into_owned(), v.into_owned())
                }
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(redacted);
    }
    url.to_string()
}

/// True when a context embeds a plaintext credential in the config file
/// itself (`url` with a password, rather than `url_env` indirection).
pub fn entry_has_plaintext_password(entry: &ContextEntry) -> bool {
    let Some(raw) = &entry.url else { return false };
    match url::Url::parse(raw) {
        Ok(url) => {
            url.password().is_some()
                || url
                    .query_pairs()
                    .any(|(k, _)| k.eq_ignore_ascii_case("password"))
        }
        // Unparseable: assume the worst — it may hide a credential.
        Err(_) => true,
    }
}

/// Warn when the config file stores a plaintext password but is readable
/// by group/other (mode is the unix permission bits; pass `None` on
/// platforms without them, which skips the check).
pub fn insecure_config_warning(config: &ConfigFile, mode: Option<u32>) -> Option<String> {
    let mode = mode?;
    if mode & 0o077 == 0 {
        return None;
    }
    let exposed: Vec<&str> = config
        .contexts
        .iter()
        .filter(|(_, entry)| entry_has_plaintext_password(entry))
        .map(|(name, _)| name.as_str())
        .collect();
    if exposed.is_empty() {
        return None;
    }
    Some(format!(
        "warning: awa config file is mode {:o} but context(s) {} embed a plaintext password — \
         chmod 600 the file, or better, switch to `url_env` indirection",
        mode & 0o777,
        exposed.join(", "),
    ))
}

/// Default config path: `$AWA_CONFIG`, else `$XDG_CONFIG_HOME/awa/config.toml`,
/// else `~/.config/awa/config.toml`.
pub fn default_config_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("AWA_CONFIG") {
        return Some(PathBuf::from(path));
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("awa").join("config.toml"))
}

/// A config file loaded from disk, keeping its path for messages.
pub struct LoadedConfig {
    pub path: PathBuf,
    pub config: ConfigFile,
    /// Permission warning to surface once, if any.
    pub warning: Option<String>,
}

/// Load the config from the default path. A missing file is not an error —
/// contexts are opt-in.
pub fn load_from_default_path() -> Result<Option<LoadedConfig>, Box<dyn std::error::Error>> {
    let Some(path) = default_config_path() else {
        return Ok(None);
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("failed to read {}: {err}", path.display()).into()),
    };
    let config = parse_config(&text).map_err(|err| format!("{}: {err}", path.display()))?;
    let mode = file_mode(&path);
    let warning = insecure_config_warning(&config, mode);
    Ok(Some(LoadedConfig {
        path,
        config,
        warning,
    }))
}

#[cfg(unix)]
fn file_mode(path: &std::path::Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .ok()
        .map(|meta| meta.permissions().mode())
}

#[cfg(not(unix))]
fn file_mode(_path: &std::path::Path) -> Option<u32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO_CONTEXTS: &str = r#"
        default_context = "cloudsql"

        [contexts.cloudsql]
        url_env = "AWA_CLOUDSQL_URL"
        production = true

        [contexts.alloydb]
        url_env = "AWA_ALLOYDB_URL"
        production = true
    "#;

    const ONE_CONTEXT: &str = r#"
        default_context = "local"

        [contexts.local]
        url = "postgres://postgres:test@localhost:15432/awa_test"
    "#;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn cloud_env(var: &str) -> Option<String> {
        match var {
            "AWA_CLOUDSQL_URL" => Some("postgres://app@10.0.0.5:5432/awa".to_string()),
            "AWA_ALLOYDB_URL" => Some("postgres://app@10.0.0.9:5432/awa".to_string()),
            _ => None,
        }
    }

    #[test]
    fn parse_rejects_url_and_url_env_together() {
        let err = parse_config(
            r#"
            [contexts.bad]
            url = "postgres://localhost/x"
            url_env = "X"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, ContextError::InvalidContext { .. }), "{err}");
    }

    #[test]
    fn parse_rejects_context_without_target() {
        let err = parse_config("[contexts.bad]\nproduction = true").unwrap_err();
        assert!(matches!(err, ContextError::InvalidContext { .. }), "{err}");
    }

    #[test]
    fn parse_rejects_unknown_default_context() {
        let err = parse_config("default_context = \"nope\"").unwrap_err();
        assert!(
            matches!(err, ContextError::UnknownDefaultContext(_)),
            "{err}"
        );
    }

    #[test]
    fn parse_rejects_unknown_keys() {
        // Typos in safety-relevant keys (e.g. `prodction`) must fail loudly,
        // not silently leave the gate open.
        let err = parse_config(
            r#"
            [contexts.prod]
            url_env = "X"
            prodction = true
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, ContextError::Parse(_)), "{err}");
    }

    #[test]
    fn cli_flag_wins_over_everything() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let target = resolve_target(
            Some("postgres://flag@localhost/db"),
            Some("alloydb"),
            Some("postgres://env@localhost/db"),
            Some(&config),
            Access::Mutating,
            cloud_env,
        )
        .unwrap();
        assert_eq!(target.source, TargetSource::CliFlag);
        assert_eq!(target.url, "postgres://flag@localhost/db");
        assert!(!target.production);
    }

    #[test]
    fn named_context_resolves_url_env_and_carries_production() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let target = resolve_target(
            None,
            Some("alloydb"),
            Some("postgres://env@localhost/db"),
            Some(&config),
            Access::Mutating,
            cloud_env,
        )
        .unwrap();
        assert_eq!(target.source, TargetSource::NamedContext);
        assert_eq!(target.url, "postgres://app@10.0.0.9:5432/awa");
        assert_eq!(target.context.as_deref(), Some("alloydb"));
        assert!(target.production);
    }

    #[test]
    fn named_context_with_unset_url_env_fails() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let err = resolve_target(
            None,
            Some("alloydb"),
            None,
            Some(&config),
            Access::ReadOnly,
            no_env,
        )
        .unwrap_err();
        assert!(matches!(err, ContextError::UrlEnvUnset { .. }), "{err}");
    }

    #[test]
    fn unknown_context_fails() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let err = resolve_target(
            None,
            Some("staging"),
            None,
            Some(&config),
            Access::ReadOnly,
            no_env,
        )
        .unwrap_err();
        assert!(matches!(err, ContextError::UnknownContext(_)), "{err}");
    }

    #[test]
    fn env_database_url_ok_for_read_only_with_many_contexts() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let target = resolve_target(
            None,
            None,
            Some("postgres://env@localhost/db"),
            Some(&config),
            Access::ReadOnly,
            cloud_env,
        )
        .unwrap();
        assert_eq!(target.source, TargetSource::EnvDatabaseUrl);
    }

    #[test]
    fn env_database_url_rejected_for_mutation_with_many_contexts() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let err = resolve_target(
            None,
            None,
            Some("postgres://env@localhost/db"),
            Some(&config),
            Access::Mutating,
            cloud_env,
        )
        .unwrap_err();
        assert!(
            matches!(err, ContextError::ExplicitContextRequired),
            "{err}"
        );
    }

    #[test]
    fn default_context_ok_for_read_only_but_not_mutation_with_many_contexts() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let target =
            resolve_target(None, None, None, Some(&config), Access::ReadOnly, cloud_env).unwrap();
        assert_eq!(target.source, TargetSource::DefaultContext);
        assert_eq!(target.context.as_deref(), Some("cloudsql"));
        assert!(target.production);

        let err = resolve_target(None, None, None, Some(&config), Access::Mutating, cloud_env)
            .unwrap_err();
        assert!(
            matches!(err, ContextError::ExplicitContextRequired),
            "{err}"
        );
    }

    #[test]
    fn single_context_default_allows_mutation() {
        // With exactly one context there is no ambiguity to guard against.
        let config = parse_config(ONE_CONTEXT).unwrap();
        let target =
            resolve_target(None, None, None, Some(&config), Access::Mutating, no_env).unwrap();
        assert_eq!(target.source, TargetSource::DefaultContext);
        assert_eq!(target.context.as_deref(), Some("local"));
    }

    #[test]
    fn env_database_url_still_works_without_config() {
        // Pre-context behaviour is unchanged for users with no config file.
        let target = resolve_target(
            None,
            None,
            Some("postgres://env@localhost/db"),
            None,
            Access::Mutating,
            no_env,
        )
        .unwrap();
        assert_eq!(target.source, TargetSource::EnvDatabaseUrl);
    }

    #[test]
    fn nothing_resolves_to_a_clear_error() {
        let err = resolve_target(None, None, None, None, Access::ReadOnly, no_env).unwrap_err();
        assert!(matches!(err, ContextError::NoDatabaseUrl), "{err}");
    }

    #[test]
    fn redact_strips_userinfo_password() {
        assert_eq!(
            redact_url("postgres://app:s3cret@db.example.com:5432/awa"),
            "postgres://app:REDACTED@db.example.com:5432/awa"
        );
    }

    #[test]
    fn redact_strips_password_query_param() {
        let redacted = redact_url("postgres://db.example.com/awa?sslmode=require&password=s3cret");
        assert!(redacted.contains("sslmode=require"), "{redacted}");
        assert!(redacted.contains("password=REDACTED"), "{redacted}");
        assert!(!redacted.contains("s3cret"), "{redacted}");
    }

    #[test]
    fn redact_never_echoes_unparseable_urls() {
        let redacted = redact_url("not a url with s3cret inside");
        assert!(!redacted.contains("s3cret"), "{redacted}");
    }

    #[test]
    fn describe_shows_context_and_production() {
        let config = parse_config(TWO_CONTEXTS).unwrap();
        let target = resolve_target(
            None,
            Some("alloydb"),
            None,
            Some(&config),
            Access::Mutating,
            cloud_env,
        )
        .unwrap();
        let line = target.describe();
        assert!(line.contains("context 'alloydb'"), "{line}");
        assert!(line.contains("[production]"), "{line}");
        assert!(line.contains("10.0.0.9"), "{line}");
    }

    #[test]
    fn insecure_warning_fires_only_for_plaintext_password_and_loose_mode() {
        let config = parse_config(
            r#"
            [contexts.leaky]
            url = "postgres://app:s3cret@localhost/awa"
            "#,
        )
        .unwrap();
        // Loose mode + plaintext password → warn (without echoing the secret).
        let warning = insecure_config_warning(&config, Some(0o644)).unwrap();
        assert!(warning.contains("leaky"), "{warning}");
        assert!(!warning.contains("s3cret"), "{warning}");
        // 0600 → no warning.
        assert!(insecure_config_warning(&config, Some(0o600)).is_none());
        // Unknown mode (non-unix) → no warning.
        assert!(insecure_config_warning(&config, None).is_none());

        // url_env indirection never warns, whatever the mode.
        let indirect = parse_config("[contexts.a]\nurl_env = \"X\"").unwrap();
        assert!(insecure_config_warning(&indirect, Some(0o644)).is_none());
    }
}
