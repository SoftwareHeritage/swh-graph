FROM openjdk:12

# Java global config
ARG MAX_RAM=2800G
ENV JAVA_TOOL_OPTIONS                                                   \
    -Xmx${MAX_RAM} -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G    \
    -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA        \
    -XX:+UseTLAB -XX:+ResizeTLAB                                        \
    -Dlogback.configurationFile=app/configuration/logback.xml

# Monitoring
RUN yum install -y time

ARG WEBGRAPH_VERSION=3.5.1
ARG LAW_VERSION=2.6.0

# Download third party binaries and dependencies
WORKDIR /srv/softwareheritage/graph/lib

RUN curl -O http://webgraph.di.unimi.it/webgraph-big-${WEBGRAPH_VERSION}-bin.tar.gz
RUN tar xvfz webgraph-big-${WEBGRAPH_VERSION}-bin.tar.gz
RUN cp webgraph-big-${WEBGRAPH_VERSION}/webgraph-big-${WEBGRAPH_VERSION}.jar .

RUN curl -O http://webgraph.di.unimi.it/webgraph-big-deps.tar.gz
RUN tar xvfz webgraph-big-deps.tar.gz

RUN curl -O http://law.di.unimi.it/software/download/law-${LAW_VERSION}-bin.tar.gz
RUN tar xvfz law-${LAW_VERSION}-bin.tar.gz
RUN cp law-${LAW_VERSION}/law-${LAW_VERSION}.jar .

# Add user files
WORKDIR /srv/softwareheritage/graph/app
COPY configuration configuration/
COPY scripts scripts/

# Default dir
WORKDIR /srv/softwareheritage/graph
