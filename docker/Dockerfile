FROM rust:slim-bookworm

# Install swh.graph (both Python and Java parts)
RUN apt-get update && \
    apt-get install --yes \
        curl zstd build-essential pkg-config libsystemd-dev \
        libpq-dev python3-dev python3-venv libclang-dev \
        zstd protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /srv/softwareheritage/venv

ENV PATH="/srv/softwareheritage/venv/bin:${PATH}"

RUN pip3 install swh.graph

RUN cargo install swh-graph swh-graph-grpc-server --all-features --locked

RUN echo "graph:\n  compress:\n    rust_executable_dir: /usr/local/cargo/bin" \
    > /srv/softwareheritage/config.yml

ENV SWH_GRAPH_CONFIG_FILE=/srv/softwareheritage/config.yml

# Default dir
WORKDIR /srv/softwareheritage/graph
