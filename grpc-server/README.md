# swh-graph-grpc-server

[gRPC](https://grpc.io/) service to run fast queries to the Software Heritage archive graph,
using a compressed in-memory representation

## Environment variables

`RUST_LOG` to set the log level, eg. `RUST_LOG=debug,h2=info`. See the
[tracing_subscriber::filter::EnvFilter](https://docs.rs/tracing-subscriber/0.3.18/tracing_subscriber/filter/struct.EnvFilter.html)
documentation for details.

`SWH_SENTRY_DSN` and `SWH_SENTRY_ENVIRONMENT`: [Sentry](https://sentry.io/) configuration, see
[Sentry Data Source Name](https://docs.sentry.io/concepts/key-terms/dsn-explainer/) and
[Sentry Environment](https://docs.sentry.io/concepts/key-terms/environments/) documentations for detail.

`SWH_SENTRY_DISABLE_LOGGING_EVENTS`: If set to `true`, does not send ERROR log statements as an
event to Sentry. Logs are sent as breadcrumbs regardless of this value.

`STATSD_HOST` and `STATSD_PORT`: default values if `--statsd-host` is not given. See
[StatsD metrics](https://docs.softwareheritage.org/devel/swh-graph/grpc-api.html#swh-graph-grpc-statsd-metrics)
for the list of StatsD metrics exported by this service.
