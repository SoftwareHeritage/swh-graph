FROM openjdk:11-slim-buster

# Install swh.graph (both Python and Java parts)
RUN apt-get update && \
    apt-get install --no-install-recommends --yes \
        curl zstd \
        gcc pkg-config libsystemd-dev python3-dev \
        python3-pip python3-setuptools python3-wheel && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install swh.graph

# Default dir
WORKDIR /srv/softwareheritage/graph
