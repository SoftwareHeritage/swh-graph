/*
 * Copyright (c) 2020-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.labels.DirEntry;
import org.softwareheritage.graph.labels.SwhLabel;
import org.softwareheritage.graph.maps.NodeIdMap;
import org.softwareheritage.graph.utils.ForkJoinBigQuickSort2;
import org.softwareheritage.graph.utils.ForkJoinQuickSort3;
import org.softwareheritage.graph.AllowedNodes;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class LabelMapBuilder {
    final static Logger logger = LoggerFactory.getLogger(LabelMapBuilder.class);

    // Create one thread per processor.
    final static int numThreads = Runtime.getRuntime().availableProcessors();
    // Allocate up to 40% of maximum memory.
    final static int DEFAULT_BATCH_SIZE = Math
            .min((int) (Runtime.getRuntime().maxMemory() * 0.4 / (numThreads * 8 * 3)), Arrays.MAX_ARRAY_SIZE);

    ORCGraphDataset dataset;
    String graphPath;
    String outputGraphPath;
    String tmpDir;
    int batchSize;

    long numNodes;
    long numArcs;

    NodeIdMap nodeIdMap;
    Object2LongFunction<byte[]> filenameMph;
    long numFilenames;
    int totalLabelWidth;

    public LabelMapBuilder(ORCGraphDataset dataset, String graphPath, String outputGraphPath, int batchSize,
            String tmpDir) throws IOException {
        this.dataset = dataset;
        this.graphPath = graphPath;
        this.outputGraphPath = (outputGraphPath == null) ? graphPath : outputGraphPath;
        this.batchSize = batchSize;
        this.tmpDir = tmpDir;

        ImmutableGraph graph = ImmutableGraph.loadOffline(graphPath);
        this.numArcs = graph.numArcs();
        this.numNodes = graph.numNodes();

        this.nodeIdMap = new NodeIdMap(graphPath);

        filenameMph = NodeIdMap.loadMph(graphPath + ".labels.mph");
        numFilenames = getMPHSize(filenameMph);
        totalLabelWidth = DirEntry.labelWidth(numFilenames);
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(LabelMapBuilder.class.getName(), "", new Parameter[]{
                    new UnflaggedOption("dataset", JSAP.STRING_PARSER, JSAP.REQUIRED, "Path to the ORC graph dataset"),
                    new UnflaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.REQUIRED, "Basename of the output graph"),
                    new FlaggedOption("outputGraphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'o',
                            "output-graph", "Basename of the output graph, same as --graph if not specified"),
                    new FlaggedOption("batchSize", JSAP.INTEGER_PARSER, String.valueOf(DEFAULT_BATCH_SIZE),
                            JSAP.NOT_REQUIRED, 'b', "batch-size", "Number of triplets held in memory in each batch"),
                    new FlaggedOption("tmpDir", JSAP.STRING_PARSER, "tmp", JSAP.NOT_REQUIRED, 'T', "temp-dir",
                            "Temporary directory path"),
                    new FlaggedOption("allowedNodeTypes", JSAP.STRING_PARSER, "*", JSAP.NOT_REQUIRED, 'N',
                            "allowed-node-types",
                            "Node types to include in the graph, eg. 'ori,snp,rel,rev' to exclude directories and contents"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JSAPResult config = parse_args(args);
        String datasetPath = config.getString("dataset");
        String graphPath = config.getString("graphPath");
        AllowedNodes allowedNodeTypes = new AllowedNodes(config.getString("allowedNodeTypes"));
        String outputGraphPath = config.getString("outputGraphPath");
        int batchSize = config.getInt("batchSize");
        String tmpDir = config.getString("tmpDir");

        ORCGraphDataset dataset = new ORCGraphDataset(datasetPath, allowedNodeTypes);

        LabelMapBuilder builder = new LabelMapBuilder(dataset, graphPath, outputGraphPath, batchSize, tmpDir);

        builder.computeLabelMap();
    }

    static long getMPHSize(Object2LongFunction<byte[]> mph) {
        return (mph instanceof Size64) ? ((Size64) mph).size64() : mph.size();
    }

    void computeLabelMap() throws IOException {
        File tempDirFile = new File(tmpDir);
        ObjectArrayList<File> forwardBatches = new ObjectArrayList<>();
        ObjectArrayList<File> backwardBatches = new ObjectArrayList<>();
        genSortedBatches(forwardBatches, backwardBatches, tempDirFile);

        BatchEdgeLabelLineIterator forwardBatchHeapIterator = new BatchEdgeLabelLineIterator(forwardBatches);
        writeLabels(forwardBatchHeapIterator, graphPath, outputGraphPath);
        for (File batch : forwardBatches) {
            batch.delete();
        }

        BatchEdgeLabelLineIterator backwardBatchHeapIterator = new BatchEdgeLabelLineIterator(backwardBatches);
        writeLabels(backwardBatchHeapIterator, graphPath + "-transposed", outputGraphPath + "-transposed");
        for (File batch : backwardBatches) {
            batch.delete();
        }

        logger.info("Done");
    }

    void genSortedBatches(ObjectArrayList<File> forwardBatches, ObjectArrayList<File> backwardBatches, File tempDirFile)
            throws IOException {
        logger.info("Initializing batch arrays.");
        long[][] srcArrays = new long[numThreads][batchSize];
        long[][] dstArrays = new long[numThreads][batchSize];
        long[][] labelArrays = new long[numThreads][batchSize];
        int[] indexes = new int[numThreads];
        long[] progressCounts = new long[numThreads];

        ProgressLogger plSortingBatches = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plSortingBatches.itemsName = "edges";
        plSortingBatches.expectedUpdates = this.numArcs;
        plSortingBatches.start("Reading edges and writing sorted batches.");

        AtomicInteger nextThreadId = new AtomicInteger(0);
        ThreadLocal<Integer> threadLocalId = ThreadLocal.withInitial(nextThreadId::getAndIncrement);

        readHashedEdgeLabels((src, dst, label, perms) -> {
            // System.err.println("0. Input " + src + " " + dst + " " + label + " " + perms);
            int threadId = threadLocalId.get();
            int idx = indexes[threadId]++;
            srcArrays[threadId][idx] = src;
            dstArrays[threadId][idx] = dst;
            labelArrays[threadId][idx] = DirEntry.toEncoded(label, perms);
            if (++progressCounts[threadId] > 1000) {
                synchronized (plSortingBatches) {
                    plSortingBatches.update(progressCounts[threadId]);
                }
                progressCounts[threadId] = 0;
            }

            if (idx == batchSize - 1) {
                processBidirectionalBatches(batchSize, srcArrays[threadId], dstArrays[threadId], labelArrays[threadId],
                        tempDirFile, forwardBatches, backwardBatches);
                indexes[threadId] = 0;
            }
        });

        IntStream.range(0, numThreads).parallel().forEach(t -> {
            int idx = indexes[t];
            if (idx > 0) {
                try {
                    processBidirectionalBatches(idx, srcArrays[t], dstArrays[t], labelArrays[t], tempDirFile,
                            forwardBatches, backwardBatches);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // Trigger the GC to free up the large arrays
        for (int i = 0; i < numThreads; i++) {
            srcArrays[i] = null;
            dstArrays[i] = null;
            labelArrays[i] = null;
        }

        logger.info("Created " + forwardBatches.size() + " forward batches and " + backwardBatches.size()
                + " backward batches.");
    }

    void readHashedEdgeLabels(GraphDataset.HashedEdgeCallback cb) throws IOException {
        ForkJoinPool forkJoinPool = new ForkJoinPool(numThreads);
        try {
            forkJoinPool.submit(() -> {
                try {
                    dataset.readEdges((node) -> {
                    }, (src, dst, label, perms) -> {
                        if (label == null) {
                            return;
                        }
                        long srcNode = nodeIdMap.getNodeId(src);
                        long dstNode = nodeIdMap.getNodeId(dst);
                        long labelId = filenameMph.getLong(label);
                        cb.onHashedEdge(srcNode, dstNode, labelId, perms);
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    void processBidirectionalBatches(final int n, final long[] source, final long[] target, final long[] labels,
            final File tempDir, final List<File> forwardBatches, final List<File> backwardBatches) throws IOException {
        processBatch(n, source, target, labels, tempDir, forwardBatches);
        processBatch(n, target, source, labels, tempDir, backwardBatches);
    }

    void processBatch(final int n, final long[] source, final long[] target, final long[] labels, final File tempDir,
            final List<File> batches) throws IOException {
        if (n == 0) {
            return;
        }
        ForkJoinQuickSort3.parallelQuickSort(source, target, labels, 0, n);

        final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
        batchFile.deleteOnExit();
        batches.add(batchFile);
        final OutputBitStream batch = new OutputBitStream(batchFile);

        // Compute unique triplets
        int u = 1;
        for (int i = n - 1; i-- != 0;) {
            if (source[i] != source[i + 1] || target[i] != target[i + 1] || labels[i] != labels[i + 1]) {
                u++;
            }
        }
        batch.writeDelta(u);

        // Write batch
        long prevSource = source[0];
        batch.writeLongDelta(prevSource);
        batch.writeLongDelta(target[0]);
        batch.writeLongDelta(labels[0]);
        // System.err.println("1. Wrote " + prevSource + " " + target[0] + " " + labels[0]);

        for (int i = 1; i < n; i++) {
            if (source[i] != prevSource) {
                // Default case, we write (source - prevsource, target, label)
                batch.writeLongDelta(source[i] - prevSource);
                batch.writeLongDelta(target[i]);
                batch.writeLongDelta(labels[i]);
                prevSource = source[i];
            } else if (target[i] != target[i - 1] || labels[i] != labels[i - 1]) {
                // Case where source is identical with prevsource, but target or label differ.
                // We write (0, target - prevtarget, label)
                batch.writeLongDelta(0);
                batch.writeLongDelta(target[i] - target[i - 1]);
                batch.writeLongDelta(labels[i]);
            } else {
                continue;
            }
            // System.err.println("1. Wrote " + source[i] + " " + target[i] + " " + labels[i]);
        }
        batch.close();
    }

    void writeLabels(EdgeLabelLineIterator mapLines, String graphBasename, String outputGraphBasename)
            throws IOException {
        // Loading the graph to iterate
        ImmutableGraph graph = ImmutableGraph.loadMapped(graphBasename);

        // Get the sorted output and write the labels and label offsets
        ProgressLogger plLabels = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plLabels.itemsName = "edges";
        plLabels.expectedUpdates = this.numArcs;
        plLabels.start("Writing the labels to the label file: " + outputGraphBasename + "-labelled.*");

        OutputBitStream labels = new OutputBitStream(
                new File(outputGraphBasename + "-labelled" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION));
        OutputBitStream offsets = new OutputBitStream(new File(
                outputGraphBasename + "-labelled" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION));
        offsets.writeGamma(0);

        EdgeLabelLine line = new EdgeLabelLine(-1, -1, -1, -1);

        NodeIterator it = graph.nodeIterator();
        boolean started = false;

        ArrayList<DirEntry> labelBuffer = new ArrayList<>(128);
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            long bits = 0;
            LazyLongIterator s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                while (line != null && line.srcNode <= srcNode && line.dstNode <= dstNode) {
                    if (line.srcNode == srcNode && line.dstNode == dstNode) {
                        labelBuffer.add(new DirEntry(line.filenameId, line.permission));
                    }

                    if (!mapLines.hasNext())
                        break;

                    line = mapLines.next();
                    if (!started) {
                        plLabels.start("Writing label map to file...");
                        started = true;
                    }
                }

                SwhLabel l = new SwhLabel("edgelabel", totalLabelWidth, labelBuffer.toArray(new DirEntry[0]));
                labelBuffer.clear();
                bits += l.toBitStream(labels, -1);
                plLabels.lightUpdate();
            }
            offsets.writeLongGamma(bits);
        }

        labels.close();
        offsets.close();
        plLabels.done();

        graph = null;

        PrintWriter pw = new PrintWriter(new FileWriter(outputGraphBasename + "-labelled.properties"));
        pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
        pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + SwhLabel.class.getName()
                + "(DirEntry," + totalLabelWidth + ")");
        pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = "
                + Paths.get(outputGraphBasename).getFileName());
        pw.close();
    }

    public static class EdgeLabelLine {
        public long srcNode;
        public long dstNode;
        public long filenameId;
        public int permission;

        public EdgeLabelLine(long labelSrcNode, long labelDstNode, long labelFilenameId, int labelPermission) {
            this.srcNode = labelSrcNode;
            this.dstNode = labelDstNode;
            this.filenameId = labelFilenameId;
            this.permission = labelPermission;
        }
    }

    public abstract static class EdgeLabelLineIterator implements Iterator<EdgeLabelLine> {
        @Override
        public abstract boolean hasNext();

        @Override
        public abstract EdgeLabelLine next();
    }

    public static class BatchEdgeLabelLineIterator extends EdgeLabelLineIterator {
        private static final int STD_BUFFER_SIZE = 128 * 1024;

        private final InputBitStream[] batchIbs;
        private final int[] inputStreamLength;
        private final long[] refArray;
        private final LongHeapSemiIndirectPriorityQueue queue;
        private final long[] prevTarget;

        /** The last returned node (-1 if no node has been returned yet). */
        private long lastNode;
        private long[][] lastNodeSuccessors = LongBigArrays.EMPTY_BIG_ARRAY;
        private long[][] lastNodeLabels = LongBigArrays.EMPTY_BIG_ARRAY;
        private long lastNodeOutdegree;
        private long lastNodeCurrentSuccessor;

        public BatchEdgeLabelLineIterator(final List<File> batches) throws IOException {
            this.batchIbs = new InputBitStream[batches.size()];
            this.refArray = new long[batches.size()];
            this.prevTarget = new long[batches.size()];
            this.queue = new LongHeapSemiIndirectPriorityQueue(refArray);
            this.inputStreamLength = new int[batches.size()];

            for (int i = 0; i < batches.size(); i++) {
                batchIbs[i] = new InputBitStream(batches.get(i), STD_BUFFER_SIZE);
                this.inputStreamLength[i] = batchIbs[i].readDelta();
                this.refArray[i] = batchIbs[i].readLongDelta();
                queue.enqueue(i);
            }

            this.lastNode = -1;
            this.lastNodeOutdegree = 0;
            this.lastNodeCurrentSuccessor = 0;
        }

        public boolean hasNextNode() {
            return !queue.isEmpty();
        }

        private void readNextNode() throws IOException {
            assert hasNext();

            int i;
            lastNode++;
            lastNodeOutdegree = 0;
            lastNodeCurrentSuccessor = 0;

            /*
             * We extract elements from the queue as long as their target is equal to last. If during the
             * process we exhaust a batch, we close it.
             */
            while (!queue.isEmpty() && refArray[i = queue.first()] == lastNode) {
                lastNodeSuccessors = BigArrays.grow(lastNodeSuccessors, lastNodeOutdegree + 1);
                lastNodeLabels = BigArrays.grow(lastNodeLabels, lastNodeOutdegree + 1);

                long target = prevTarget[i] += batchIbs[i].readLongDelta();
                long label = batchIbs[i].readLongDelta();
                BigArrays.set(lastNodeSuccessors, lastNodeOutdegree, target);
                BigArrays.set(lastNodeLabels, lastNodeOutdegree, label);

                // System.err.println("2. Read " + lastNode + " " + target + " " + label);
                if (--inputStreamLength[i] == 0) {
                    queue.dequeue();
                    batchIbs[i].close();
                    batchIbs[i] = null;
                } else {
                    // We read a new source and update the queue.
                    final long sourceDelta = batchIbs[i].readLongDelta();
                    if (sourceDelta != 0) {
                        refArray[i] += sourceDelta;
                        prevTarget[i] = 0;
                        queue.changed();
                    }
                }
                lastNodeOutdegree++;
            }

            // Neither quicksort nor heaps are stable, so we reestablish order here.
            // LongBigArrays.radixSort(lastNodeSuccessors, lastNodeLabels, 0, lastNodeOutdegree);
            ForkJoinBigQuickSort2.parallelQuickSort(lastNodeSuccessors, lastNodeLabels, 0, lastNodeOutdegree);
        }

        @Override
        public boolean hasNext() {
            return lastNodeCurrentSuccessor < lastNodeOutdegree || hasNextNode();
        }

        @Override
        public EdgeLabelLine next() {
            if (lastNode == -1 || lastNodeCurrentSuccessor >= lastNodeOutdegree) {
                try {
                    do {
                        readNextNode();
                    } while (hasNextNode() && lastNodeOutdegree == 0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            long src = lastNode;
            long dst = BigArrays.get(lastNodeSuccessors, lastNodeCurrentSuccessor);
            long compressedLabel = BigArrays.get(lastNodeLabels, lastNodeCurrentSuccessor);
            long labelName = DirEntry.labelNameFromEncoded(compressedLabel);
            int permission = DirEntry.permissionFromEncoded(compressedLabel);
            // System.err.println("3. Output (encoded): " + src + " " + dst + " " + compressedLabel);
            // System.err.println("4. Output (decoded): " + src + " " + dst + " " + labelName + " " +
            // permission);
            lastNodeCurrentSuccessor++;
            return new EdgeLabelLine(src, dst, labelName, permission);
        }
    }
}
