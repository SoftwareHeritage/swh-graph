package org.softwareheritage.graph;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import org.junit.Assert;
import org.junit.BeforeClass;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import org.softwareheritage.graph.Graph;

public class GraphTest {
    static Graph graph;

    public static <T> void assertEqualsAnyOrder(Collection<T> expecteds, Collection<T> actuals) {
        Assert.assertThat(expecteds, containsInAnyOrder(actuals.toArray()));
    }

    @BeforeClass
    public static void setUp() throws IOException {
        Path graphPath = Paths.get("..", "..", "swh", "graph", "tests", "dataset", "output", "example");
        graph = new Graph(graphPath.toString());
    }

    public Graph getGraph() {
        return graph;
    }
}
