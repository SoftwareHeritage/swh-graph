package org.softwareheritage.graph.rpc;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Rule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SWHID;
import org.softwareheritage.graph.SwhBidirectionalGraph;

import java.util.ArrayList;
import java.util.Iterator;

public class TraversalServiceTest extends GraphTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private static Server server;
    private static ManagedChannel channel;
    protected static SwhBidirectionalGraph g;
    protected static TraversalServiceGrpc.TraversalServiceBlockingStub client;

    @BeforeAll
    static void setup() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        g = GraphServer.loadGraph(getGraphPath().toString());
        server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(new GraphServer.TraversalService(g.copy())).build().start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        client = TraversalServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void teardown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    public ArrayList<SWHID> getSWHIDs(Iterator<Node> it) {
        ArrayList<SWHID> res = new ArrayList<>();
        it.forEachRemaining((Node n) -> {
            res.add(new SWHID(n.getSwhid()));
        });
        return res;
    }
}
