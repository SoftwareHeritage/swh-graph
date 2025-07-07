# swh-subgrapher

`swh-subgrapher` is a Rust script designed to assist in the generation of Software Heritage Subgraphs.
It takes a list of input origin URLs, and returns every SWHID reachable from these origins
using [`swh-graph`](https://crates.io/crates/swh-graph).

This can be used to [generate subdatasets](https://docs.softwareheritage.org/devel/swh-export/generate_subdataset.html), ie. custom, smaller datasets from the vast Software Heritage archive.

## Operation

The script performs the following main functions:

This tool reads a list of origin URLs from a specified input file.
If an origin is not found, and the `--allow-protocol-variations / --p` flag is set, it will attempt to find the origin by switching between `git://` and `https://` protocols.
Then it writes all reachable SWHIDs to a file (`--output` flag), with each SWHID on a new line.
If a origin could not be found in the graph, its URL is written to a `origin_errors.txt` file in the same path of the output.

## Prerequisites

* Rust programming language and Cargo (its package manager).
* A local copy of a Software Heritage graph dataset. You can find information on how to obtain this on the [Software Heritage documentation](https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html#).
  * The result will be a sugraph of the one used as input.
* This uses the `swh-graph` library, and it requires some system dependencies not provided by cargo. Check the [swh-graph quickstart](https://docs.softwareheritage.org/devel/swh-graph/quickstart.html) docs for more information.

## Usage

To run the script, you need to provide the path to the Software Heritage graph dataset and the path to a file containing the list of origin URLs.

```bash
cargo run --release --bin swh-subgrapher -- --graph /path/to/dataset/2024-12-06-history-hosting/graph -t --origins origins.txt --output results
```

### Debugging

If facing issues, try running with DEBUG logs to get a more detailed view of what is happening:

```bash
RUST_LOG=debug swh-subgrapher ...
```
