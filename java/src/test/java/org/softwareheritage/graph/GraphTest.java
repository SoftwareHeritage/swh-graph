package org.softwareheritage.graph;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class GraphTest {
    static Graph graph;

    @BeforeAll
    public static void setUp() throws IOException {
        Path graphPath = Paths.get("..", "swh", "graph", "tests", "dataset", "output", "example");
        graph = Graph.loadMapped(graphPath.toString());
    }

    public Graph getGraph() {
        return graph;
    }

    public static SWHID fakeSWHID(String type, int num) {
        return new SWHID(String.format("swh:1:%s:%040d", type, num));
    }

    public static <T> void assertEqualsAnyOrder(Collection<T> expecteds, Collection<T> actuals) {
        MatcherAssert.assertThat(expecteds, containsInAnyOrder(actuals.toArray()));
    }

    public static ArrayList<Long> lazyLongIteratorToList(LazyLongIterator input) {
        ArrayList<Long> inputList = new ArrayList<>();
        Iterator<Long> inputIt = LazyLongIterators.eager(input);
        inputIt.forEachRemaining(inputList::add);
        return inputList;
    }
}
