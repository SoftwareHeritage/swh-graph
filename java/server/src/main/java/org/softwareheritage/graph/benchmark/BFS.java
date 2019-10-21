package org.softwareheritage.graph.benchmark;

import org.softwareheritage.graph.Graph;

import com.martiansoftware.jsap.*;

import it.unimi.dsi.big.webgraph.algo.ParallelBreadthFirstVisit;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.law.big.graph.BFS;

import java.io.IOException;


public class ParallelBFS {
    private Graph graph;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename);
        System.err.println("Graph loaded.");
    }

    public static long[][] bfsperm(final ImmutableGraph graph, final long startingNode, final long[][] startPerm, int bufferSize) throws IOException {
    final long n = graph.numNodes();
    // Allow enough memory to behave like in-memory queue
    bufferSize = Math.min(bufferSize, (int)Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * n));

    final long[][] visitOrder = LongBigArrays.newBigArray(n);
    LongBigArrays.fill(visitOrder, -1);
    final long[][] invStartPerm = startPerm == null ? null : Util.invertPermutation(startPerm, LongBigArrays.newBigArray(n));
    // Use a disk based queue to store BFS frontier
    final File queueFile = File.createTempFile(BFS.class.getSimpleName(), "queue");
    final ByteDiskQueue queue = ByteDiskQueue.createNew(queueFile, bufferSize, true);
    final byte[] byteBuf = new byte[Long.BYTES];
    // WARNING: no 64-bit version of this data-structure, but it can support
    // indices up to 2^37
    final LongArrayBitVector visited = LongArrayBitVector.ofLength(n);
    final ProgressLogger pl = new ProgressLogger(LOGGER);
    pl.expectedUpdates = n;
    pl.itemsName = "nodes";
    pl.start("Starting breadth-first visit...");

    long pos = 0;

    if (startPerm != null) {
        for (long i = 0; i < n; i++) {
            final long start = i == 0 && startingNode != -1 ? startingNode : LongBigArrays.get(invStartPerm, i);
            if (visited.getBoolean(start)) continue;
            queue.enqueue(Longs.toByteArray(start));
            visited.set(start);

            final LongArrayList successors = new LongArrayList();

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);

                LongBigArrays.set(visitOrder, pos++, currentNode);
                final LazyLongIterator iterator = graph.successors(currentNode);

                successors.clear();
                long succ;
                while((succ = iterator.nextLong()) != -1) {
                    if (!visited.getBoolean(succ)) {
                        successors.add(succ);
                        visited.set(succ);
                    }
                }

                final long[] randomSuccessors = successors.elements();
                LongArrays.quickSort(randomSuccessors, 0, successors.size(), (x, y) -> {
                    return Long.compare(LongBigArrays.get(startPerm, y), LongBigArrays.get(startPerm, x));
                });

                for (int j = successors.size(); j-- != 0;)
                    queue.enqueue(Longs.toByteArray(randomSuccessors[j]));
                pl.update();
            }

            if (startingNode != -1) break;
        }
    }
    else {
        for (long i = 0; i < n; i++) {
            final long start = i == 0 && startingNode != -1 ? startingNode : i;
            if (visited.getBoolean(start)) continue;
            queue.enqueue(Longs.toByteArray(start));
            visited.set(start);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);

                LongBigArrays.set(visitOrder, pos++, currentNode);
                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while((succ = iterator.nextLong()) != -1) {
                    if (!visited.getBoolean(succ)) {
                        visited.set(succ);
                        queue.enqueue(Longs.toByteArray(succ));
                        

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(
                ParallelBFS.class.getName(),
                "",
                new Parameter[] {
                    new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            'g', "graph", "Basename of the compressed graph"),

                    new FlaggedOption("numThreads", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            't', "numthreads", "Number of threads"),

                    new FlaggedOption("useTransposed", JSAP.BOOLEAN_PARSER, "false", JSAP.NOT_REQUIRED,
                            'T', "transposed", "Use transposed graph (default: false)"),
                }
            );

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) {
        JSAPResult config = parse_args(args);
        String graphPath = config.getString("graphPath");
        int numThreads = config.getInt("numThreads");
        boolean useTransposed = config.getBoolean("useTransposed");

        ProgressLogger logger = new ProgressLogger();
        long startTime;
        double totalTime;


        ParallelBFS bfs = new ParallelBFS();
        try {
            bfs.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        BFS.bfsperm(bfs.graph.getBVGraph(useTransposed), -1, Arrays.MAX_ARRAY_SIZE ^ ~0x7);
        ParallelBreadthFirstVisit visit =
            new ParallelBreadthFirstVisit(bfs.graph.getBVGraph(useTransposed),
                                          numThreads, true, logger);
        logger.start("Parallel BFS visit...");
        visit.visitAll();
        logger.done();
    }
}
