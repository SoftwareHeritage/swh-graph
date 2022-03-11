package org.softwareheritage.graph.compress;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
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
import org.softwareheritage.graph.utils.ForkJoinQuickSort3;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LabelMapBuilder {
    final static String SORT_BUFFER_SIZE = "40%";
    final static Logger logger = LoggerFactory.getLogger(LabelMapBuilder.class);

    final static String ALGORITHM = "quicksort";

    String orcDatasetPath;
    String graphPath;
    String outputGraphPath;
    int batchSize;
    String tmpDir;

    ImmutableGraph graph;
    long numNodes;
    long numArcs;

    NodeIdMap nodeIdMap;
    Object2LongFunction<byte[]> filenameMph;
    long numFilenames;
    int totalLabelWidth;

    public LabelMapBuilder(String orcDatasetPath, String graphPath, String outputGraphPath, int batchSize,
            String tmpDir) throws IOException {
        this.orcDatasetPath = orcDatasetPath;
        this.graphPath = graphPath;
        this.outputGraphPath = (outputGraphPath == null) ? graphPath : outputGraphPath;
        this.batchSize = batchSize;
        this.tmpDir = tmpDir;

        this.graph = ImmutableGraph.loadMapped(graphPath);
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
                    new FlaggedOption("batchSize", JSAP.INTEGER_PARSER, "10000000", JSAP.NOT_REQUIRED, 'b',
                            "batch-size", "Number of triplets held in memory in each batch"),
                    new FlaggedOption("tmpDir", JSAP.STRING_PARSER, "tmp", JSAP.NOT_REQUIRED, 'T', "temp-dir",
                            "Temporary directory path"),});

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
        String orcDataset = config.getString("dataset");
        String graphPath = config.getString("graphPath");
        String outputGraphPath = config.getString("outputGraphPath");
        int batchSize = config.getInt("batchSize");
        String tmpDir = config.getString("tmpDir");

        LabelMapBuilder builder = new LabelMapBuilder(orcDataset, graphPath, outputGraphPath, batchSize, tmpDir);

        logger.info("Loading graph and MPH functions...");
        builder.computeLabelMap();
    }

    static long getMPHSize(Object2LongFunction<byte[]> mph) {
        return (mph instanceof Size64) ? ((Size64) mph).size64() : mph.size();
    }

    void computeLabelMap() throws IOException, InterruptedException {
        switch (ALGORITHM) {
            case "quicksort":
                this.computeLabelMapQuicksort();
                break;
            case "gnusort":
                this.computeLabelMapSort();
                break;
            case "bsort":
                this.computeLabelMapBsort();
                break;
        }
    }

    void computeLabelMapSort() throws IOException {
        // Pass the intermediate representation to sort(1) so that we see the labels in the order they will
        // appear in the label file.
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("sort", "-k1,1n", "-k2,2n", // Numerical sort
                "--numeric-sort", "--buffer-size", SORT_BUFFER_SIZE, "--temporary-directory", tmpDir);
        Process sort = processBuilder.start();
        BufferedOutputStream sort_stdin = new BufferedOutputStream(sort.getOutputStream());
        FastBufferedInputStream sort_stdout = new FastBufferedInputStream(sort.getInputStream());

        readHashedEdgeLabels((src, dst, label, perms) -> sort_stdin
                .write((src + "\t" + dst + "\t" + label + "\t" + perms + "\n").getBytes(StandardCharsets.US_ASCII)));
        sort_stdin.close();

        EdgeLabelLineIterator mapLines = new TextualEdgeLabelLineIterator(sort_stdout);
        writeLabels(mapLines);
        logger.info("Done");
    }

    void computeLabelMapBsort() throws IOException, InterruptedException {
        // Pass the intermediate representation to bsort(1) so that we see the labels in the order they will
        // appear in the label file.

        String tmpFile = tmpDir + "/labelsToSort.bin";
        final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpFile)));

        // Number of bytes to represent a node.
        final int nodeBytes = (Long.SIZE - Long.numberOfLeadingZeros(graph.numNodes())) / 8 + 1;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        logger.info("Writing labels to a packed binary files (node bytes: {})", nodeBytes);

        readHashedEdgeLabels((src, dst, label, perms) -> {
            buffer.putLong(0, src);
            out.write(buffer.array(), Long.BYTES - nodeBytes, nodeBytes);
            buffer.putLong(0, dst);
            out.write(buffer.array(), Long.BYTES - nodeBytes, nodeBytes);
            out.writeLong(label);
            out.writeInt(perms);
        });

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("/home/seirl/bsort/src/bsort", "-v", "-r",
                String.valueOf(nodeBytes * 2 + Long.BYTES + Integer.BYTES), "-k", String.valueOf(nodeBytes * 2),
                tmpFile);
        Process sort = processBuilder.start();
        sort.waitFor();

        final DataInputStream sortedLabels = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpFile)));
        BinaryEdgeLabelLineIterator mapLines = new BinaryEdgeLabelLineIterator(sortedLabels, nodeBytes);
        writeLabels(mapLines);

        logger.info("Done");
    }

    void computeLabelMapQuicksort() throws IOException {
        File tempDirFile = new File(tmpDir);
        ObjectArrayList<File> batches = new ObjectArrayList<>();

        ProgressLogger plSortingBatches = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plSortingBatches.itemsName = "edges";
        plSortingBatches.expectedUpdates = this.numArcs;
        plSortingBatches.start("Sorting batches.");

        long[] srcArray = new long[batchSize];
        long[] dstArray = new long[batchSize];
        long[] labelArray = new long[batchSize];
        AtomicInteger i = new AtomicInteger(0);

        readHashedEdgeLabels((src, dst, label, perms) -> {
            // System.err.println("0. Input " + src + " " + dst + " " + label + " " + perms);
            int idx = i.getAndAdd(1);
            srcArray[idx] = src;
            dstArray[idx] = dst;
            labelArray[idx] = DirEntry.toEncoded(label, perms);
            plSortingBatches.lightUpdate();

            if (idx == batchSize - 1) {
                processBatch(batchSize, srcArray, dstArray, labelArray, tempDirFile, batches);
                i.set(0);
            }
        });

        if (i.get() != 0) {
            processBatch(i.get(), srcArray, dstArray, labelArray, tempDirFile, batches);
        }

        plSortingBatches.logger().info("Created " + batches.size() + " batches");

        BatchEdgeLabelLineIterator batchHeapIterator = new BatchEdgeLabelLineIterator(batches);
        writeLabels(batchHeapIterator);

        logger.info("Done");
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

    void readHashedEdgeLabels(GraphDataset.HashedEdgeCallback cb) throws IOException {
        ORCGraphDataset dataset = new ORCGraphDataset(orcDatasetPath);
        FastBufferedOutputStream out = new FastBufferedOutputStream(System.out);
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
        out.flush();
    }

    void writeLabels(EdgeLabelLineIterator mapLines) throws IOException {
        // Get the sorted output and write the labels and label offsets
        ProgressLogger plLabels = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plLabels.itemsName = "edges";
        plLabels.expectedUpdates = this.numArcs;
        plLabels.start("Writing the labels to the label file.");

        OutputBitStream labels = new OutputBitStream(
                new File(outputGraphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION));
        OutputBitStream offsets = new OutputBitStream(
                new File(outputGraphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION));
        offsets.writeGamma(0);

        EdgeLabelLine line = new EdgeLabelLine(-1, -1, -1, -1);

        NodeIterator it = graph.nodeIterator();
        boolean started = false;

        ArrayList<DirEntry> labelBuffer = new ArrayList<>(128);
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            int bits = 0;
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
            offsets.writeGamma(bits);
        }

        labels.close();
        offsets.close();
        plLabels.done();

        PrintWriter pw = new PrintWriter(new FileWriter(outputGraphPath + "-labelled.properties"));
        pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
        pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + SwhLabel.class.getName()
                + "(DirEntry," + totalLabelWidth + ")");
        pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = "
                + Paths.get(outputGraphPath).getFileName());
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

    public static class ScannerEdgeLabelLineIterator extends EdgeLabelLineIterator {
        private final Scanner scanner;

        public ScannerEdgeLabelLineIterator(InputStream input) {
            this.scanner = new Scanner(input);
        }

        @Override
        public boolean hasNext() {
            return this.scanner.hasNext();
        }

        @Override
        public EdgeLabelLine next() {
            String line = scanner.nextLine();
            String[] parts = line.split("\\t");
            return new EdgeLabelLine(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]),
                    Integer.parseInt(parts[3]));

            /*
             * String line = scanner.nextLine(); long src = scanner.nextLong(); long dst = scanner.nextLong();
             * long label = scanner.nextLong(); int permission = scanner.nextInt(); return new
             * EdgeLabelLine(src, dst, label, permission);
             */
        }
    }

    public static class TextualEdgeLabelLineIterator extends EdgeLabelLineIterator {
        private final FastBufferedInputStream input;
        private final Charset charset;
        private byte[] array;
        boolean finished;

        public TextualEdgeLabelLineIterator(FastBufferedInputStream input) {
            this.input = input;
            this.finished = false;
            this.charset = StandardCharsets.US_ASCII;
            this.array = new byte[1024];
        }

        @Override
        public boolean hasNext() {
            return !this.finished;
        }

        @Override
        public EdgeLabelLine next() {
            int start = 0, len;
            try {
                while ((len = input.readLine(array, start, array.length - start,
                        FastBufferedInputStream.ALL_TERMINATORS)) == array.length - start) {
                    start += len;
                    array = ByteArrays.grow(array, array.length + 1);
                }
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            if (len == -1) {
                finished = true;
                return null;
            }
            final int lineLength = start + len;

            int offset = 0;

            // Scan source id.
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            long src = Long.parseLong(new String(array, start, offset - start, charset));

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;

            // Scan target ID
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            long dst = Long.parseLong(new String(array, start, offset - start, charset));

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;

            // Scan label
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            long label = Long.parseLong(new String(array, start, offset - start, charset));

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;

            // Scan permission
            int permission = 0;
            if (offset < lineLength) {
                start = offset;
                while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                    offset++;
                permission = Integer.parseInt(new String(array, start, offset - start, charset));
            }

            return new EdgeLabelLine(src, dst, label, permission);
        }
    }

    public static class BinaryEdgeLabelLineIterator extends EdgeLabelLineIterator {
        private final int nodeBytes;
        DataInputStream stream;
        boolean finished;
        ByteBuffer buffer;

        public BinaryEdgeLabelLineIterator(DataInputStream stream, int nodeBytes) {
            this.stream = stream;
            this.nodeBytes = nodeBytes;
            this.buffer = ByteBuffer.allocate(Long.BYTES);
            this.finished = false;
        }

        @Override
        public boolean hasNext() {
            return !finished;
        }

        @Override
        public EdgeLabelLine next() {
            try {
                // long src = stream.readLong();
                // long dst = stream.readLong();
                stream.readFully(this.buffer.array(), Long.BYTES - nodeBytes, nodeBytes);
                this.buffer.position(0);
                long src = this.buffer.getLong();
                this.buffer.clear();

                stream.readFully(this.buffer.array(), Long.BYTES - nodeBytes, nodeBytes);
                this.buffer.position(0);
                long dst = this.buffer.getLong();
                this.buffer.clear();

                long label = stream.readLong();
                int perm = stream.readInt();
                return new EdgeLabelLine(src, dst, label, perm);
            } catch (EOFException e) {
                finished = true;
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
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
            LongBigArrays.radixSort(lastNodeSuccessors, lastNodeLabels, 0, lastNodeOutdegree);
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
