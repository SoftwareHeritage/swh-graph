FROM openjdk:11

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

# temporary, SWH-specific version of law, waiting for changes to be integrated
# upstream
RUN curl -o law-2.5.1.jar https://forge.softwareheritage.org/file/download/jizfo55fndw54iwr645c/PHID-FILE-kjtrkzrwotlkh6mtkazj/law-2.5.1_swh1.jar

# Add user files
WORKDIR /swh/app
COPY configuration configuration/
COPY scripts scripts/
