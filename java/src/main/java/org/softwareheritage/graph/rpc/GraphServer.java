/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import com.google.protobuf.FieldMask;
import com.martiansoftware.jsap.*;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.stub.StreamObserver;
import io.grpc.protobuf.services.ProtoReflectionService;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.SWHID;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.compress.LabelMapBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import javax.json.JsonReader;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonParsingException;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class GraphServer {
    private final static Logger logger = LoggerFactory.getLogger(GraphServer.class);

    private final SwhBidirectionalGraph graph;
    private final int port;
    private final int threads;
    private Server server;

    /**
     * @param graphBasename the basename of the SWH graph to load
     * @param port the port on which the GRPC server will listen
     * @param threads the number of threads to use in the server threadpool
     */
    public GraphServer(String graphBasename, int port, int threads) throws IOException {
        this.graph = loadGraph(graphBasename);
        this.port = port;
        this.threads = threads;
    }

    /** Load a graph and all its properties. */
    public static SwhBidirectionalGraph loadGraph(String basename) throws IOException {
        SwhBidirectionalGraph g = SwhBidirectionalGraph.loadLabelledMapped(basename, new ProgressLogger(logger));
        g.loadContentLength();
        g.loadContentIsSkipped();
        g.loadPersonIds();
        g.loadAuthorTimestamps();
        g.loadCommitterTimestamps();
        g.loadMessages();
        g.loadTagNames();
        g.loadLabelNames();
        return g;
    }

    /** Start the RPC server. */
    private void start() throws IOException {
        server = NettyServerBuilder.forPort(port).withChildOption(ChannelOption.SO_REUSEADDR, true)
                .executor(Executors.newFixedThreadPool(threads)).addService(new TraversalService(graph))
                .addService(ProtoReflectionService.newInstance()).build().start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                GraphServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static JSAPResult parseArgs(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(LabelMapBuilder.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("port", JSAP.INTEGER_PARSER, "50091", JSAP.NOT_REQUIRED, 'p', "port",
                                    "The port on which the server should listen."),
                            new FlaggedOption("threads", JSAP.INTEGER_PARSER, "0", JSAP.NOT_REQUIRED, 't', "threads",
                                    "The number of concurrent threads. 0 = number of cores."),
                            new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.REQUIRED,
                                    "Basename of the output graph")});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    /** Main launches the server from the command line. */
    public static void main(String[] args) throws IOException, InterruptedException {
        JSAPResult config = parseArgs(args);
        String graphBasename = config.getString("graphBasename");
        int port = config.getInt("port");
        int threads = config.getInt("threads");
        if (threads == 0) {
            threads = Runtime.getRuntime().availableProcessors();
        }

        final GraphServer server = new GraphServer(graphBasename, port, threads);
        server.start();
        server.blockUntilShutdown();
    }

    /** Implementation of the Traversal service, which contains all the graph querying endpoints. */
    static class TraversalService extends TraversalServiceGrpc.TraversalServiceImplBase {
        SwhBidirectionalGraph graph;

        public TraversalService(SwhBidirectionalGraph graph) {
            this.graph = graph;
        }

        /** Return various statistics on the overall graph. */
        @Override
        public void stats(StatsRequest request, StreamObserver<StatsResponse> responseObserver) {
            StatsResponse.Builder response = StatsResponse.newBuilder();
            response.setNumNodes(graph.numNodes());
            response.setNumEdges(graph.numArcs());

            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(graph.getPath() + ".properties"));
                properties.load(new FileInputStream(graph.getPath() + ".stats"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            response.setCompressionRatio(Double.parseDouble(properties.getProperty("compratio")));
            response.setBitsPerNode(Double.parseDouble(properties.getProperty("bitspernode")));
            response.setBitsPerEdge(Double.parseDouble(properties.getProperty("bitsperlink")));
            response.setAvgLocality(Double.parseDouble(properties.getProperty("avglocality")));
            response.setIndegreeMin(Long.parseLong(properties.getProperty("minindegree")));
            response.setIndegreeMax(Long.parseLong(properties.getProperty("maxindegree")));
            response.setIndegreeAvg(Double.parseDouble(properties.getProperty("avgindegree")));
            response.setOutdegreeMin(Long.parseLong(properties.getProperty("minoutdegree")));
            response.setOutdegreeMax(Long.parseLong(properties.getProperty("maxoutdegree")));
            response.setOutdegreeAvg(Double.parseDouble(properties.getProperty("avgoutdegree")));
            try {
                JsonReader jsonReader = Json.createReader(
                        new FileInputStream(Paths.get(graph.getPath()).resolveSibling("meta/export.json").toString()));
                JsonObject object = jsonReader.readObject();
                jsonReader.close();
                long exportStartedAt = OffsetDateTime.parse(object.getString("export_start")).toInstant()
                        .getEpochSecond();
                long exportEndedAt = OffsetDateTime.parse(object.getString("export_end")).toInstant().getEpochSecond();
                response.setExportStartedAt(exportStartedAt);
                response.setExportEndedAt(exportEndedAt);
            } catch (IOException | JsonParsingException | DateTimeParseException e) {
                // let’s leave exportStartedAt to 0 if we can’t figure out the right value
                logger.warn("Unable to read or parse `export_start` or `export_end` in `meta/export.json`", e);
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        /** Return a single node and its properties. */
        @Override
        public void getNode(GetNodeRequest request, StreamObserver<Node> responseObserver) {
            SwhBidirectionalGraph g = graph.copy();
            long nodeId;
            try {
                nodeId = g.getNodeId(new SWHID(request.getSwhid()));
            } catch (IllegalArgumentException e) {
                responseObserver
                        .onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
                return;
            }
            Node.Builder builder = Node.newBuilder();
            NodePropertyBuilder.buildNodeProperties(g.getForwardGraph(), request.hasMask() ? request.getMask() : null,
                    builder, nodeId);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        /** Perform a BFS traversal from a set of source nodes and stream the nodes encountered. */
        @Override
        public void traverse(TraversalRequest request, StreamObserver<Node> responseObserver) {
            SwhBidirectionalGraph g = graph.copy();
            Traversal.SimpleTraversal t;
            try {
                t = new Traversal.SimpleTraversal(g, request, responseObserver::onNext);
            } catch (IllegalArgumentException e) {
                responseObserver
                        .onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
                return;
            }
            t.visit();
            responseObserver.onCompleted();
        }

        /**
         * Find the shortest path between a set of source nodes and a node that matches a given criteria
         * using a BFS.
         */
        @Override
        public void findPathTo(FindPathToRequest request, StreamObserver<Path> responseObserver) {
            SwhBidirectionalGraph g = graph.copy();
            Traversal.FindPathTo t;
            try {
                t = new Traversal.FindPathTo(g, request);
            } catch (IllegalArgumentException e) {
                responseObserver
                        .onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
                return;
            }
            t.visit();
            Path path = t.getPath();
            if (path == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
            } else {
                responseObserver.onNext(path);
                responseObserver.onCompleted();
            }
        }

        /**
         * Find the shortest path between a set of source nodes and a set of destination nodes using a
         * bidirectional BFS.
         */
        @Override
        public void findPathBetween(FindPathBetweenRequest request, StreamObserver<Path> responseObserver) {
            SwhBidirectionalGraph g = graph.copy();
            Traversal.FindPathBetween t;
            try {
                t = new Traversal.FindPathBetween(g, request);
            } catch (IllegalArgumentException e) {
                responseObserver
                        .onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
                return;
            }
            t.visit();
            Path path = t.getPath();
            if (path == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
            } else {
                responseObserver.onNext(path);
                responseObserver.onCompleted();
            }
        }

        /** Return the number of nodes traversed by a BFS traversal. */
        @Override
        public void countNodes(TraversalRequest request, StreamObserver<CountResponse> responseObserver) {
            AtomicLong count = new AtomicLong(0);
            SwhBidirectionalGraph g = graph.copy();
            TraversalRequest fixedReq = TraversalRequest.newBuilder(request)
                    // Ignore return fields, just count nodes
                    .setMask(FieldMask.getDefaultInstance()).build();
            Traversal.SimpleTraversal t;
            try {
                t = new Traversal.SimpleTraversal(g, fixedReq, n -> count.incrementAndGet());
            } catch (IllegalArgumentException e) {
                responseObserver
                        .onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
                return;
            }
            t.visit();
            CountResponse response = CountResponse.newBuilder().setCount(count.get()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /** Return the number of edges traversed by a BFS traversal. */
        @Override
        public void countEdges(TraversalRequest request, StreamObserver<CountResponse> responseObserver) {
            AtomicLong count = new AtomicLong(0);
            SwhBidirectionalGraph g = graph.copy();
            TraversalRequest fixedReq = TraversalRequest.newBuilder(request)
                    // Force return empty successors to count the edges
                    .setMask(FieldMask.newBuilder().addPaths("num_successors").build()).build();
            Traversal.SimpleTraversal t;
            try {
                t = new Traversal.SimpleTraversal(g, fixedReq, n -> count.addAndGet(n.getNumSuccessors()));
            } catch (IllegalArgumentException e) {
                responseObserver
                        .onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).withCause(e).asException());
                return;
            }
            t.visit();
            CountResponse response = CountResponse.newBuilder().setCount(count.get()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
