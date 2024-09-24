// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;

use anyhow::{anyhow, bail, Context, Result};
use std::net::{ToSocketAddrs, UdpSocket};
use std::str::FromStr;

use cadence::{StatsdClient, UdpMetricSink};
macro_rules! resolve_host {
    ($host:expr) => {
        $host
            .to_socket_addrs()
            .context("Invalid --statsd-host address")?
            .next()
            .context("Could not resolve --statsd-host address")?
    };
}

#[allow(clippy::comparison_to_empty)]
/// Parses the `$STATSD_TAGS` environment variable as a comma-separated list of `key=value`.
fn parse_statsd_tags<'a>(
    tags: &'a str,
    env: &'a HashMap<String, String>,
) -> Result<Vec<(&'a str, &'a str)>> {
    if tags == "" {
        return Ok(Vec::new());
    }

    // TODO: Add support for variable expansion, like swh.core.statsd does.
    tags.split(",")
        .map(|tag| {
            let Some((k, mut v)) = tag.split_once(':') else {
                bail!("STATSD_TAGS needs to be in 'key:value' format, not {tag}");
            };
            if v.starts_with('$') {
                // Replace the tag value with an environment variable.
                // Mimics behavior of swh/core/statsd.py, which also only supports substitution,
                // and not expansion in general.
                v = &v[1..];
                if v.starts_with('{') && v.ends_with('}') {
                    v = &v[1..v.len() - 1];
                }
                v = env.get(v).ok_or(anyhow!(
                    "STATSD_TAGS references env variable {v}, which is undefined"
                ))?;
            }
            Ok((k, v))
        })
        .collect()
}

/// Given an optional `<host>:<port>`, returns a [`StatsdClient`]
///
/// If `<host>:<port>` is not provided, defaults to `localhost:8125` (or whatever is
/// configured by the `STATSD_HOST` and `STATSD_PORT` environment variables).
pub fn statsd_client(host: Option<String>) -> Result<StatsdClient> {
    let socket = UdpSocket::bind("[::]:0").unwrap();
    let default_host = (
        std::env::var("STATSD_HOST").unwrap_or("localhost".to_owned()),
        u16::from_str(&std::env::var("STATSD_PORT").unwrap_or("8125".to_owned()))
            .context("Invalid STATSD_PORT value")?,
    );
    let host = match host {
        Some(host) => resolve_host!(host),
        None => resolve_host!(default_host),
    };
    let sink = UdpMetricSink::from(host, socket).unwrap();
    let client = parse_statsd_tags(
        &std::env::var("STATSD_TAGS").unwrap_or("".to_string()),
        &std::env::vars().collect(),
    )?
    .into_iter()
    .fold(
        StatsdClient::builder("swh_graph_grpc_server", sink),
        |client_builder, (k, v)| client_builder.with_tag(k, v),
    )
    .with_error_handler(|e| log::error!("Could not update Statsd metric: {e}"))
    .build();

    Ok(client)
}

#[test]
fn test_parse_statsd_tags() -> Result<()> {
    let env = HashMap::new();

    assert_eq!(
        parse_statsd_tags("", &env).context("Could not parse empty string")?,
        Vec::new()
    );

    assert_eq!(
        parse_statsd_tags("foo:bar", &env).context("Could not parse foo:bar")?,
        vec![("foo", "bar")]
    );

    assert_eq!(
        parse_statsd_tags("foo:", &env).context("Could not parse foo:bar")?,
        vec![("foo", "")]
    );

    assert_eq!(
        parse_statsd_tags("foo:bar,baz:qux", &env).context("Could not parse foo:bar,baz:qux")?,
        vec![("foo", "bar"), ("baz", "qux")]
    );

    assert_eq!(
        parse_statsd_tags("foo:bar:bar2,baz:qux", &env)
            .context("Could not parse foo:bar,baz:qux")?,
        vec![("foo", "bar:bar2"), ("baz", "qux")]
    );

    Ok(())
}

#[test]
fn test_parse_statsd_tags_fail() -> Result<()> {
    let env = HashMap::new();

    assert!(parse_statsd_tags("foo", &env).is_err());

    Ok(())
}

#[test]
fn test_parse_statsd_tags_var_substitution() -> Result<()> {
    let mut env = HashMap::new();
    env.insert("ENV_VAR".to_string(), "ENV_VALUE".to_string());

    assert_eq!(
        parse_statsd_tags("foo:ENV_VAR", &env).context("Could not parse foo:bar")?,
        vec![("foo", "ENV_VAR")] // not substituted
    );

    assert_eq!(
        parse_statsd_tags("foo:$ENV_VAR", &env).context("Could not parse foo:bar")?,
        vec![("foo", "ENV_VALUE")]
    );

    assert_eq!(
        parse_statsd_tags("foo:${ENV_VAR}", &env).context("Could not parse foo:bar")?,
        vec![("foo", "ENV_VALUE")]
    );

    assert_eq!(
        parse_statsd_tags("foo:${ENV_VAR},bar:baz", &env).context("Could not parse foo:bar")?,
        vec![("foo", "ENV_VALUE"), ("bar", "baz")]
    );

    assert!(parse_statsd_tags("foo:$NOT_ENV_VAR", &env).is_err());
    assert!(parse_statsd_tags("foo:${ENV_VAR", &env).is_err());

    Ok(())
}
