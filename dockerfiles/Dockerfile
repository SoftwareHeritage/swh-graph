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

# Download third party binaries and dependencies
WORKDIR /srv/softwareheritage/graph/lib

RUN curl -O http://webgraph.di.unimi.it/webgraph-big-3.5.1-bin.tar.gz
RUN tar xvfz webgraph-big-3.5.1-bin.tar.gz
RUN cp webgraph-big-3.5.1/webgraph-big-3.5.1.jar .

RUN curl -O http://webgraph.di.unimi.it/webgraph-big-deps.tar.gz
RUN tar xvfz webgraph-big-deps.tar.gz

RUN curl -O http://law.di.unimi.it/software/download/law-2.5.2-bin.tar.gz
RUN tar xvfz law-2.5.2-bin.tar.gz
RUN cp law-2.5.2/law-2.5.2.jar .

# Add user files
WORKDIR /srv/softwareheritage/graph/app
COPY configuration configuration/
COPY scripts scripts/

# Default dir
WORKDIR /srv/softwareheritage/graph
