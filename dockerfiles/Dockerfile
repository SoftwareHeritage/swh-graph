FROM maven:3.6.0-jdk-11
WORKDIR /graph-lib

# Download webgraph binary
RUN curl -O http://webgraph.di.unimi.it/webgraph-big-3.5.0-bin.tar.gz
RUN tar xvfz webgraph-big-3.5.0-bin.tar.gz
RUN cp webgraph-big-3.5.0/webgraph-big-3.5.0.jar .

# Download webgraph dependencies
RUN curl -O http://webgraph.di.unimi.it/webgraph-big-deps.tar.gz
RUN tar xvfz webgraph-big-deps.tar.gz

# Monitoring
RUN apt-get update
RUN apt-get install -y time

WORKDIR /graph
COPY scripts scripts/
