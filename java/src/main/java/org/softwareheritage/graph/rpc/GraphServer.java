package org.softwareheritage.graph.rpc;

import com.martiansoftware.jsap.*;
import io.grpc.Server;
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
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class GraphServer {
    private final static Logger logger = LoggerFactory.getLogger(GraphServer.class);

    private final SwhBidirectionalGraph graph;
    private final int port;
    private final int threads;
    private Server server;

    public GraphServer(String graphBasename, int port, int threads) throws IOException {
        // TODO: use loadLabelledMapped() when https://github.com/vigna/webgraph-big/pull/5 is merged
        this.graph = SwhBidirectionalGraph.loadLabelled(graphBasename, new ProgressLogger(logger));
        this.port = port;
        this.threads = threads;
        graph.loadContentLength();
        graph.loadContentIsSkipped();
        graph.loadPersonIds();
        graph.loadAuthorTimestamps();
        graph.loadCommitterTimestamps();
        graph.loadMessages();
        graph.loadTagNames();
        graph.loadLabelNames();
    }

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
                            new FlaggedOption("threads", JSAP.INTEGER_PARSER, "1", JSAP.NOT_REQUIRED, 't', "threads",
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

    /**
     * Main launches the server from the command line.
     */
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

    static class TraversalService extends TraversalServiceGrpc.TraversalServiceImplBase {
        SwhBidirectionalGraph graph;

        public TraversalService(SwhBidirectionalGraph graph) {
            this.graph = graph;
        }

        @Override
        public void checkSwhid(CheckSwhidRequest request, StreamObserver<CheckSwhidResponse> responseObserver) {
            boolean exists = true;
            CheckSwhidResponse.Builder builder = CheckSwhidResponse.newBuilder().setExists(true);
            try {
                graph.getNodeId(new SWHID(request.getSwhid()));
            } catch (IllegalArgumentException e) {
                builder.setExists(false);
                builder.setDetails(e.getMessage());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

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
            response.setCompression(Double.parseDouble(properties.getProperty("compratio")));
            response.setBitsPerNode(Double.parseDouble(properties.getProperty("bitspernode")));
            response.setBitsPerEdge(Double.parseDouble(properties.getProperty("bitsperlink")));
            response.setAvgLocality(Double.parseDouble(properties.getProperty("avglocality")));
            response.setIndegreeMin(Long.parseLong(properties.getProperty("minindegree")));
            response.setIndegreeMax(Long.parseLong(properties.getProperty("maxindegree")));
            response.setIndegreeAvg(Double.parseDouble(properties.getProperty("avgindegree")));
            response.setOutdegreeMin(Long.parseLong(properties.getProperty("minoutdegree")));
            response.setOutdegreeMax(Long.parseLong(properties.getProperty("maxoutdegree")));
            response.setOutdegreeAvg(Double.parseDouble(properties.getProperty("avgoutdegree")));
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        @Override
        public void traverse(TraversalRequest request, StreamObserver<Node> responseObserver) {
            SwhBidirectionalGraph g = graph.copy();
            Traversal.simpleTraversal(g, request, responseObserver::onNext);
            responseObserver.onCompleted();
        }

        @Override
        public void countNodes(TraversalRequest request, StreamObserver<CountResponse> responseObserver) {
            AtomicInteger count = new AtomicInteger(0);
            SwhBidirectionalGraph g = graph.copy();
            TraversalRequest fixedReq = TraversalRequest.newBuilder(request)
                    // Ignore return fields, just count nodes
                    .setReturnFields(NodeFields.getDefaultInstance()).build();
            Traversal.simpleTraversal(g, fixedReq, (Node node) -> {
                count.incrementAndGet();
            });
            CountResponse response = CountResponse.newBuilder().setCount(count.get()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void countEdges(TraversalRequest request, StreamObserver<CountResponse> responseObserver) {
            AtomicInteger count = new AtomicInteger(0);
            SwhBidirectionalGraph g = graph.copy();
            TraversalRequest fixedReq = TraversalRequest.newBuilder(request)
                    // Force return empty successors to count the edges
                    .setReturnFields(NodeFields.newBuilder().setSuccessor(true).setSuccessorSwhid(false).build())
                    .build();
            Traversal.simpleTraversal(g, fixedReq, (Node node) -> {
                count.addAndGet(node.getSuccessorCount());
            });
            CountResponse response = CountResponse.newBuilder().setCount(count.get()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
