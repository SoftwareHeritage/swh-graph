package org.softwareheritage.graph.experiments.collabgraph;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.Transform;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhBidirectionalGraph;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class GenCollabGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenCollabGraph.class);

    static private final int DEFAULT_NUM_THREADS = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_BATCH_SIZE = Math
            .min((int) (Runtime.getRuntime().maxMemory() * 0.4 / (DEFAULT_NUM_THREADS * 8 * 2)), Arrays.MAX_ARRAY_SIZE);

    private static JSAPResult parseArgs(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(GenCollabGraph.class.getName(), "",
                    new Parameter[]{
                            new UnflaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                    JSAP.NOT_GREEDY, "The basename of the compressed graph."),
                            new UnflaggedOption("collabGraphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                    JSAP.NOT_GREEDY, "The basename of the output collaboration graph"),
                            new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T',
                                    "temp-dir", "A directory for all temporary batch files."),
                            new FlaggedOption("numThreads", JSAP.INTEGER_PARSER, String.valueOf(DEFAULT_NUM_THREADS),
                                    JSAP.NOT_REQUIRED, 't', "numthreads", "Number of threads"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    private static boolean isBaseRevision(SwhBidirectionalGraph g, long node) {
        if (g.getNodeType(node) != Node.Type.REV)
            return false;

        final LazyLongIterator iterator = g.successors(node);
        long succ;
        while ((succ = iterator.nextLong()) != -1) {
            if (g.getNodeType(succ) == Node.Type.REV)
                return false;
        }
        return true;
    }

    private static Set<Long> getAuthorClique(SwhBidirectionalGraph g, Long baseNode) {
        final Deque<Long> stack = new ArrayDeque<>();
        HashSet<Long> seen = new HashSet<>();
        HashSet<Long> authors = new HashSet<>();

        stack.push(baseNode);
        seen.add(baseNode);
        while (!stack.isEmpty()) {
            final Long currentNode = stack.pop();
            final LazyLongIterator iterator = g.predecessors(currentNode);
            long succ;
            while ((succ = iterator.nextLong()) != -1) {
                if (!seen.contains(succ)) {
                    // We only care about the revision graph
                    if (g.getNodeType(succ) != Node.Type.REV) {
                        continue;
                    }

                    authors.add(g.getAuthorId(succ));

                    stack.push(succ);
                    seen.add(succ);
                }
            }
        }
        return authors;
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        JSAPResult config = parseArgs(args);
        String graphPath = config.getString("graphPath");
        String collabGraphBasename = config.getString("collabGraphPath");
        int numThreads = config.getInt("numThreads");
        File tempDir = new File(config.getString("tempDir"));

        final ProgressLogger pl = new ProgressLogger(LOGGER);

        SwhBidirectionalGraph graph;
        try {
            graph = SwhBidirectionalGraph.loadMapped(graphPath, pl);
            graph.loadPersonIds();
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
            return;
        }

        long[][] permutation = LongBigArrays.newBigArray(graph.numNodes());
        Util.identity(permutation);
        LongBigArrays.shuffle(permutation, new XoRoShiRo128PlusRandom());

        final ObjectArrayList<File> batches = new ObjectArrayList<>();

        final int batchSize = (DEFAULT_BATCH_SIZE % 2 == 0) ? DEFAULT_BATCH_SIZE : DEFAULT_BATCH_SIZE - 1;
        long[][] srcArrays = new long[numThreads][batchSize];
        long[][] dstArrays = new long[numThreads][batchSize];
        int[] indexes = new int[numThreads];
        AtomicInteger pairs = new AtomicInteger(0);
        long[] progressCounts = new long[numThreads];
        AtomicLong maxPersonId = new AtomicLong(0);

        AtomicInteger nextThreadId = new AtomicInteger(0);
        ThreadLocal<Integer> threadLocalId = ThreadLocal.withInitial(nextThreadId::getAndIncrement);

        long nodesPerThread = graph.numNodes() / numThreads;

        pl.start("Finding author cliques from base revisions");
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; ++i) {
            service.submit(() -> {
                int threadId = threadLocalId.get();

                SwhBidirectionalGraph g = graph.copy();
                for (long n = nodesPerThread * threadId; n < Math.min(nodesPerThread * (threadId + 1),
                        graph.numNodes()); n++) {
                    n = BigArrays.get(permutation, n);

                    if (!isBaseRevision(g, n)) {
                        continue;
                    }

                    Set<Long> authors = getAuthorClique(g, n);
                    // Iterate on all pairs of authors
                    for (long a1 : authors) {
                        maxPersonId.accumulateAndGet(a1, Math::max);
                        for (long a2 : authors) {
                            if (a1 != a2) {
                                int idx = indexes[threadId]++;
                                srcArrays[threadId][idx] = a1;
                                dstArrays[threadId][idx] = a2;

                                if (idx == batchSize - 1) {
                                    try {
                                        pairs.addAndGet(Transform.processBatch(batchSize, srcArrays[threadId],
                                                dstArrays[threadId], tempDir, batches));
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    indexes[threadId] = 0;
                                }

                                if (++progressCounts[threadId] > 1000) {
                                    synchronized (pl) {
                                        pl.update(progressCounts[threadId]);
                                        // System.err.println(java.util.Arrays.toString(progressCounts));
                                    }
                                    progressCounts[threadId] = 0;
                                }
                            }
                        }
                    }

                }
            });
        }

        service.shutdown();
        if (!service.awaitTermination(365, TimeUnit.DAYS)) {
            service.shutdownNow();
            throw new RuntimeException("Timeout");
        }
        pl.done();

        IntStream.range(0, numThreads).parallel().forEach(t -> {
            int idx = indexes[t];
            if (idx > 0) {
                try {
                    pairs.addAndGet(Transform.processBatch(idx, srcArrays[t], dstArrays[t], tempDir, batches));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Transform.BatchGraph batchGraph = new Transform.BatchGraph(maxPersonId.get(), pairs.get(), batches);
        BVGraph.store(batchGraph, collabGraphBasename, pl);
    }
}
