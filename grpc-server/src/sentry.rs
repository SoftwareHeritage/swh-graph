// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::env::VarError;

use anyhow::Result;
use sentry::integrations::log::LogFilter;
use sentry::ClientInitGuard;

/// Parses an environment variable as a boolean, like `swh.core.sentry.override_with_bool_envvar`
fn parse_bool_env_var(var_name: &'static str, default: bool) -> bool {
    _parse_bool_env_var(var_name, std::env::var(var_name), default)
}

/// Testable variant of [`parse_bool_env_var`] that takes both the variable name and its value
fn _parse_bool_env_var(
    var_name: &'static str,
    value: Result<String, VarError>,
    default: bool,
) -> bool {
    match value.as_deref() {
        Ok("t" | "true" | "y" | "yes" | "1") => true,
        Ok("f" | "false" | "n" | "no" | "0") => false,
        Ok(value) => {
            log::warn!("Could not interpret environment variable {var_name}={value} as boolean, using default value {default}");
            default
        }
        Err(VarError::NotPresent) => default,
        Err(e) => {
            log::warn!("Could not interpret environment variable {var_name} ({e}), using default value {default}");
            default
        }
    }
}

pub fn setup(logger: Box<dyn log::Log>) -> (Result<ClientInitGuard, VarError>, Box<dyn log::Log>) {
    let guard = std::env::var("SWH_SENTRY_DSN").map(|sentry_dsn| {
        sentry::init((
            sentry_dsn,
            sentry::ClientOptions {
                release: sentry::release_name!(),
                environment: std::env::var("SWH_SENTRY_ENVIRONMENT").ok().map(Into::into),
                ..Default::default()
            },
        ))
    });

    if let Err(ref e) = guard {
        log::error!("Could not initialize Sentry: {e}");
    }

    let mut logger = sentry::integrations::log::SentryLogger::with_dest(logger);

    if parse_bool_env_var("SWH_SENTRY_DISABLE_LOGGING_EVENTS", false) {
        logger = logger.filter(|md| match md.level() {
            log::Level::Error => LogFilter::Breadcrumb, // replaces the default (LogFilter::Exception)
            log::Level::Warn | log::Level::Info => LogFilter::Breadcrumb,
            log::Level::Debug | log::Level::Trace => LogFilter::Ignore,
        });
    }
    (guard, Box::new(logger))
}

#[test]
fn test_parse_bool_env_var() {
    assert!(_parse_bool_env_var("foo", Ok("t".to_owned()), false));
    assert!(_parse_bool_env_var("foo", Ok("t".to_owned()), true));
    assert!(_parse_bool_env_var("foo", Ok("yes".to_owned()), false));
    assert!(_parse_bool_env_var("foo", Ok("yes".to_owned()), true));

    assert!(!_parse_bool_env_var("foo", Ok("f".to_owned()), false));
    assert!(!_parse_bool_env_var("foo", Ok("f".to_owned()), true));
    assert!(!_parse_bool_env_var("foo", Ok("no".to_owned()), false));
    assert!(!_parse_bool_env_var("foo", Ok("no".to_owned()), true));

    assert!(!_parse_bool_env_var("foo", Ok("invalid".to_owned()), false));
    assert!(_parse_bool_env_var("foo", Ok("invalid".to_owned()), true));
    assert!(!_parse_bool_env_var("foo", Ok("invalid".to_owned()), false));
    assert!(_parse_bool_env_var("foo", Ok("invalid".to_owned()), true));

    assert!(!_parse_bool_env_var(
        "foo",
        Err(VarError::NotPresent),
        false
    ));
    assert!(_parse_bool_env_var("foo", Err(VarError::NotPresent), true));
    assert!(!_parse_bool_env_var(
        "foo",
        Err(VarError::NotPresent),
        false
    ));
    assert!(_parse_bool_env_var("foo", Err(VarError::NotPresent), true));
}
