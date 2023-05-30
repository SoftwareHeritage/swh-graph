/*
 * Copyright (c) 2022-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.big.webgraph.Transform;
import it.unimi.dsi.fastutil.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;

import org.softwareheritage.graph.AllowedNodes;

public class ScatteredArcsORCGraph extends ImmutableSequentialGraph {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScatteredArcsORCGraph.class);

    /** The default number of threads. */
    public static final int DEFAULT_NUM_THREADS = Runtime.getRuntime().availableProcessors();

    /** The default batch size. */
    public static final int DEFAULT_BATCH_SIZE = Math
            .min((int) (Runtime.getRuntime().maxMemory() * 0.4 / (DEFAULT_NUM_THREADS * 8 * 2)), Arrays.MAX_ARRAY_SIZE);

    /** The batch graph used to return node iterators. */
    private final Transform.BatchGraph batchGraph;

    /**
     * Creates a scattered-arcs ORC graph.
     *
     * @param dataset the Swh ORC Graph dataset
     * @param function an explicitly provided function from string representing nodes to node numbers,
     *            or <code>null</code> for the standard behaviour.
     * @param n the number of nodes of the graph (used only if <code>function</code> is not
     *            <code>null</code>).
     * @param numThreads the number of threads to use.
     * @param batchSize the number of integers in a batch; two arrays of integers of this size will be
     *            allocated by each thread.
     * @param tempDir a temporary directory for the batches, or <code>null</code> for
     *            {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
     * @param pl a progress logger, or <code>null</code>.
     */
    public ScatteredArcsORCGraph(final ORCGraphDataset dataset, final Object2LongFunction<byte[]> function,
            final long n, final int numThreads, final int batchSize, final File tempDir, final ProgressLogger pl)
            throws IOException {
        final ObjectArrayList<File> batches = new ObjectArrayList<>();
        ForkJoinPool forkJoinPool = new ForkJoinPool(numThreads);

        long[][] srcArrays = new long[numThreads][batchSize];
        long[][] dstArrays = new long[numThreads][batchSize];
        int[] indexes = new int[numThreads];
        long[] progressCounts = new long[numThreads];
        AtomicInteger pairs = new AtomicInteger(0);

        AtomicInteger nextThreadId = new AtomicInteger(0);
        ThreadLocal<Integer> threadLocalId = ThreadLocal.withInitial(nextThreadId::getAndIncrement);

        if (pl != null) {
            pl.itemsName = "arcs";
            pl.start("Creating sorted batches...");
        }

        try {
            forkJoinPool.submit(() -> {
                try {
                    dataset.readEdges((node) -> {
                    }, (src, dst, label, perms) -> {
                        long s = function.getLong(src);
                        long t = function.getLong(dst);

                        int threadId = threadLocalId.get();
                        int idx = indexes[threadId]++;
                        srcArrays[threadId][idx] = s;
                        dstArrays[threadId][idx] = t;

                        if (idx == batchSize - 1) {
                            pairs.addAndGet(Transform.processBatch(batchSize, srcArrays[threadId], dstArrays[threadId],
                                    tempDir, batches));
                            indexes[threadId] = 0;
                        }

                        if (pl != null && ++progressCounts[threadId] > 1000) {
                            synchronized (pl) {
                                pl.update(progressCounts[threadId]);
                            }
                            progressCounts[threadId] = 0;
                        }

                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

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

        // Trigger the GC to free up the large arrays
        for (int i = 0; i < numThreads; i++) {
            srcArrays[i] = null;
            dstArrays[i] = null;
        }

        if (pl != null) {
            pl.done();
            pl.logger().info("Created " + batches.size() + " batches.");
        }

        batchGraph = new Transform.BatchGraph(n, pairs.get(), batches);
    }

    @Override
    public long numNodes() {
        if (batchGraph == null)
            throw new UnsupportedOperationException(
                    "The number of nodes is unknown (you need to generate all the batches first).");
        return batchGraph.numNodes();
    }

    @Override
    public long numArcs() {
        if (batchGraph == null)
            throw new UnsupportedOperationException(
                    "The number of arcs is unknown (you need to generate all the batches first).");
        return batchGraph.numArcs();
    }

    @Override
    public NodeIterator nodeIterator(final long from) {
        return batchGraph.nodeIterator(from);
    }

    @Override
    public boolean hasCopiableIterators() {
        return batchGraph.hasCopiableIterators();
    }

    @Override
    public ScatteredArcsORCGraph copy() {
        return this;
    }

    @SuppressWarnings("unchecked")
    public static void main(final String[] args)
            throws IllegalArgumentException, SecurityException, IOException, JSAPException, ClassNotFoundException {
        final SimpleJSAP jsap = new SimpleJSAP(ScatteredArcsORCGraph.class.getName(),
                "Converts a scattered list of arcs from an ORC graph dataset into a BVGraph.",
                new Parameter[]{
                        new FlaggedOption("logInterval", JSAP.LONG_PARSER,
                                Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l',
                                "log-interval", "The minimum time interval between activity logs in milliseconds."),
                        new FlaggedOption("numThreads", JSAP.INTSIZE_PARSER, Integer.toString(DEFAULT_NUM_THREADS),
                                JSAP.NOT_REQUIRED, 't', "threads", "The number of threads to use."),
                        new FlaggedOption("batchSize", JSAP.INTSIZE_PARSER, Integer.toString(DEFAULT_BATCH_SIZE),
                                JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in arcs."),
                        new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T',
                                "temp-dir", "A directory for all temporary batch files."),
                        new FlaggedOption("function", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f',
                                "function",
                                "A serialised function from strings to longs that will be used to translate identifiers to node numbers."),
                        new FlaggedOption("comp", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', "comp",
                                "A compression flag (may be specified several times).")
                                        .setAllowMultipleDeclarations(true),
                        new FlaggedOption("windowSize", JSAP.INTEGER_PARSER,
                                String.valueOf(BVGraph.DEFAULT_WINDOW_SIZE), JSAP.NOT_REQUIRED, 'w', "window-size",
                                "Reference window size (0 to disable)."),
                        new FlaggedOption("maxRefCount", JSAP.INTEGER_PARSER,
                                String.valueOf(BVGraph.DEFAULT_MAX_REF_COUNT), JSAP.NOT_REQUIRED, 'm', "max-ref-count",
                                "Maximum number of backward references (-1 for ∞)."),
                        new FlaggedOption("minIntervalLength", JSAP.INTEGER_PARSER,
                                String.valueOf(BVGraph.DEFAULT_MIN_INTERVAL_LENGTH), JSAP.NOT_REQUIRED, 'i',
                                "min-interval-length", "Minimum length of an interval (0 to disable)."),
                        new FlaggedOption("zetaK", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_ZETA_K),
                                JSAP.NOT_REQUIRED, 'k', "zeta-k", "The k parameter for zeta-k codes."),
                        new FlaggedOption("allowedNodeTypes", JSAP.STRING_PARSER, "*", JSAP.NOT_REQUIRED, 'N',
                                "allowed-node-types",
                                "Node types to include in the graph, eg. 'ori,snp,rel,rev' to exclude directories and contents"),
                        new UnflaggedOption("dataset", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                JSAP.NOT_GREEDY, "The path to the ORC graph dataset."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                JSAP.NOT_GREEDY, "The basename of the output graph"),});

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted())
            System.exit(1);

        String basename = jsapResult.getString("basename");
        String orcDatasetPath = jsapResult.getString("dataset");
        AllowedNodes allowedNodeTypes = new AllowedNodes(jsapResult.getString("allowedNodeTypes"));
        ORCGraphDataset orcDataset = new ORCGraphDataset(orcDatasetPath, allowedNodeTypes);

        int flags = 0;
        for (final String compressionFlag : jsapResult.getStringArray("comp")) {
            try {
                flags |= BVGraph.class.getField(compressionFlag).getInt(BVGraph.class);
            } catch (final Exception notFound) {
                throw new JSAPException("Compression method " + compressionFlag + " unknown.");
            }
        }

        final int windowSize = jsapResult.getInt("windowSize");
        final int zetaK = jsapResult.getInt("zetaK");
        int maxRefCount = jsapResult.getInt("maxRefCount");
        if (maxRefCount == -1)
            maxRefCount = Integer.MAX_VALUE;
        final int minIntervalLength = jsapResult.getInt("minIntervalLength");

        if (!jsapResult.userSpecified("function")) {
            throw new IllegalArgumentException("Function must be specified.");
        }
        final Object2LongFunction<byte[]> function = (Object2LongFunction<byte[]>) BinIO
                .loadObject(jsapResult.getString("function"));
        long n = function instanceof Size64 ? ((Size64) function).size64() : function.size();

        File tempDir = null;
        if (jsapResult.userSpecified("tempDir")) {
            tempDir = new File(jsapResult.getString("tempDir"));
        }

        final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
        final int batchSize = jsapResult.getInt("batchSize");
        final int numThreads = jsapResult.getInt("numThreads");
        final ScatteredArcsORCGraph graph = new ScatteredArcsORCGraph(orcDataset, function, n, numThreads, batchSize,
                tempDir, pl);
        BVGraph.store(graph, basename, windowSize, maxRefCount, minIntervalLength, zetaK, flags, pl);
    }
}
