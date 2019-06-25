FROM maven:3.6.0-jdk-11

# Java global config
ARG MAX_RAM=2800G
ENV JAVA_TOOL_OPTIONS                                                   \
    -Xmx${MAX_RAM} -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G    \
    -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA        \
    -XX:+UseTLAB -XX:+ResizeTLAB                                        \
    -Dlogback.configurationFile=configuration/logback.xml

# Monitoring
RUN apt-get update
RUN apt-get install -y time

# Download third party binaries and dependencies
WORKDIR /swh/graph-lib

RUN curl -O http://webgraph.di.unimi.it/webgraph-big-3.5.1-bin.tar.gz
RUN tar xvfz webgraph-big-3.5.1-bin.tar.gz
RUN cp webgraph-big-3.5.1/webgraph-big-3.5.1.jar .

RUN curl -O http://webgraph.di.unimi.it/webgraph-big-deps.tar.gz
RUN tar xvfz webgraph-big-deps.tar.gz

RUN curl -O http://law.di.unimi.it/software/download/law-2.5.1-bin.tar.gz
RUN tar xvfz law-2.5.1-bin.tar.gz
RUN cp law-2.5.1/law-2.5.1.jar .

# Add user files
WORKDIR /swh/app
COPY configuration configuration/
COPY scripts scripts/

