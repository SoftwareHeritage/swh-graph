FROM maven:3.6.0-jdk-11
WORKDIR /app

# Download webgraph binary
RUN curl -O http://webgraph.di.unimi.it/webgraph-3.6.1-bin.tar.gz
RUN tar xvfz webgraph-3.6.1-bin.tar.gz
RUN cp webgraph-3.6.1/webgraph-3.6.1.jar .

# Download webgraph dependencies
RUN curl -O http://webgraph.di.unimi.it/webgraph-deps.tar.gz
RUN tar xvfz webgraph-deps.tar.gz

# Download LAW (for LLP ordering)
RUN curl -O http://law.di.unimi.it/software/download/law-2.5-bin.tar.gz
RUN tar xvfz law-2.5-bin.tar.gz
RUN cp law-2.5/law-2.5.jar .

WORKDIR /graph
COPY compress_graph.sh .
