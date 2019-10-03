FROM openjdk:13-slim-buster

# Java global config
ARG MAX_RAM=2800G
ENV JAVA_TOOL_OPTIONS \
    -Xmx${MAX_RAM} -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G \
    -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA \
    -XX:+UseTLAB -XX:+ResizeTLAB \
    -Dlogback.configurationFile=app/configuration/logback.xml

# Install swh.graph (both Python and Java parts)
RUN apt-get update
RUN apt-get install --no-install-recommends --yes \
    curl time \
    gcc pkg-config libsystemd-dev python3-dev \
    python3-pip python3-setuptools
RUN pip3 install swh.graph

# Install 3rd party dependencies (not shipped with swh.graph)
WORKDIR /srv/softwareheritage/graph/lib

RUN cp /usr/local/share/swh-graph/*.jar .

# law is not shipped via Maven, download from upstream
ARG LAW_VERSION=2.6.0
RUN curl -O http://law.di.unimi.it/software/download/law-${LAW_VERSION}-bin.tar.gz
RUN tar xvfz law-${LAW_VERSION}-bin.tar.gz
RUN cp law-${LAW_VERSION}/law-${LAW_VERSION}.jar .

# Add user files
WORKDIR /srv/softwareheritage/graph/app
COPY configuration configuration/
COPY scripts scripts/

# Default dir
WORKDIR /srv/softwareheritage/graph
