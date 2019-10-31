FROM openjdk:13-slim-buster

# Java global config
ARG MAX_RAM=2800G
ENV JAVA_TOOL_OPTIONS \
    -Xmx${MAX_RAM} -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G \
    -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA \
    -XX:+UseTLAB -XX:+ResizeTLAB \
    -Dlogback.configurationFile=app/configuration/logback.xml

# Install swh.graph (both Python and Java parts)
RUN apt-get update && \
    apt-get install --no-install-recommends --yes \
        curl time \
        gcc pkg-config libsystemd-dev python3-dev \
        python3-pip python3-setuptools && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install swh.graph

# Install 3rd party dependencies (not shipped with swh.graph)
WORKDIR /srv/softwareheritage/graph/lib

RUN ln /usr/local/share/swh-graph/*.jar .

# Add user files
WORKDIR /srv/softwareheritage/graph/app
COPY configuration configuration/
COPY scripts scripts/

# Default dir
WORKDIR /srv/softwareheritage/graph
