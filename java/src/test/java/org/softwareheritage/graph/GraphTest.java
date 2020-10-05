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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class GraphTest {
    static Graph graph;

    public static <T> void assertEqualsAnyOrder(Collection<T> expecteds, Collection<T> actuals) {
        MatcherAssert.assertThat(expecteds, containsInAnyOrder(actuals.toArray()));
    }

    public static void assertLazyLongIteratorsEqual(LazyLongIterator expected, LazyLongIterator actual) {
        ArrayList<Long> expectedList = new ArrayList<>();
        ArrayList<Long> actualList = new ArrayList<>();
        Iterator<Long> expectedIt = LazyLongIterators.eager(expected);
        Iterator<Long> actualIt = LazyLongIterators.eager(actual);
        expectedIt.forEachRemaining(expectedList::add);
        actualIt.forEachRemaining(actualList::add);
        Assertions.assertArrayEquals(expectedList.toArray(), actualList.toArray());
    }

    @BeforeAll
    public static void setUp() throws IOException {
        Path graphPath = Paths.get("..", "swh", "graph", "tests", "dataset", "output", "example");
        graph = new Graph(graphPath.toString());
    }

    public Graph getGraph() {
        return graph;
    }
}
