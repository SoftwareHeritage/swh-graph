package org.softwareheritage.graph;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.github.luben.zstd.ZstdInputStream;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

public class GraphTest {
    static SwhBidirectionalGraph graph;

    final protected String TEST_ORIGIN_ID = "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054";

    @BeforeAll
    public static void setUp() throws IOException {
        Path graphPath = Paths.get("..", "swh", "graph", "tests", "dataset", "compressed", "example");
        graph = SwhBidirectionalGraph.loadMapped(graphPath.toString());
    }

    public SwhBidirectionalGraph getGraph() {
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

    public static String[] readZstFile(Path zstFile) throws IOException {
        ZstdInputStream zis = new ZstdInputStream(new FileInputStream(zstFile.toFile()));
        return (new String(zis.readAllBytes())).split("\n");
    }
}
