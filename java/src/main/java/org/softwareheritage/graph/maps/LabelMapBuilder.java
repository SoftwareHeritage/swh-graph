package org.softwareheritage.graph.maps;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.labels.DirEntry;
import org.softwareheritage.graph.labels.SwhLabel;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class LabelMapBuilder {
    final static String SORT_BUFFER_SIZE = "40%";
    final static Logger logger = LoggerFactory.getLogger(LabelMapBuilder.class);

    String graphPath;
    String debugPath;
    String tmpDir;
    ImmutableGraph graph;

    Object2LongFunction<String> swhIdMph;
    long[][] orderMap;
    Object2LongFunction<String> filenameMph;
    long numFilenames;
    int totalLabelWidth;

    public LabelMapBuilder(String graphPath, String debugPath, String tmpDir) {
        this.graphPath = graphPath;
        this.debugPath = debugPath;
        this.tmpDir = tmpDir;
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(LabelMapBuilder.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),
                            new FlaggedOption("debugPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd',
                                    "debug-path", "Store the intermediate representation here for debug"),

                            new FlaggedOption("tmpDir", JSAP.STRING_PARSER, "tmp", JSAP.NOT_REQUIRED, 't', "tmp",
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

    public static void main(String[] args) throws IOException {
        JSAPResult config = parse_args(args);
        String graphPath = config.getString("graphPath");
        String tmpDir = config.getString("tmpDir");
        String debugPath = config.getString("debugPath");

        LabelMapBuilder builder = new LabelMapBuilder(graphPath, debugPath, tmpDir);
        builder.computeLabelMap();
    }

    @SuppressWarnings("unchecked") // Suppress warning for Object2LongFunction cast
    static Object2LongFunction<String> loadMPH(String mphBasename) throws IOException {
        Object2LongFunction<String> mphMap = null;
        try {
            mphMap = (Object2LongFunction<String>) BinIO.loadObject(mphBasename + ".mph");
        } catch (ClassNotFoundException e) {
            logger.error("unknown class object in .mph file: " + e);
            System.exit(2);
        }
        return mphMap;
    }

    static long getMPHSize(Object2LongFunction<String> mph) {
        return (mph instanceof Size64) ? ((Size64) mph).size64() : mph.size();
    }

    void computeLabelMap() throws IOException {
        /*
         * Pass the intermediate representation to sort(1) so that we see the labels in the order they will
         * appear in the label file.
         */
        logger.info("Loading graph and MPH functions...");
        loadGraph();

        logger.info("Hashing the input labels...");

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("sort", "-k1,1n", "-k2,2n", // Numerical sort
                "--numeric-sort", "--buffer-size", SORT_BUFFER_SIZE, "--temporary-directory", tmpDir);
        Process sort = processBuilder.start();
        BufferedOutputStream sort_stdin = new BufferedOutputStream(sort.getOutputStream());
        BufferedInputStream sort_stdout = new BufferedInputStream(sort.getInputStream());

        final FastBufferedInputStream fbis = new FastBufferedInputStream(System.in);
        hashLabelStream(fbis, sort_stdin);
        sort_stdin.close();

        logger.info("Writing label map to file...");
        writeLabels(sort_stdout);

        logger.info("Done");
    }

    void loadGraph() throws IOException {
        graph = BVGraph.loadMapped(graphPath);

        swhIdMph = loadMPH(graphPath);

        orderMap = LongBigArrays.newBigArray(getMPHSize(swhIdMph));
        BinIO.loadLongs(graphPath + ".order", orderMap);

        filenameMph = loadMPH(graphPath + "-labels");
        numFilenames = getMPHSize(filenameMph);
        totalLabelWidth = DirEntry.labelWidth(numFilenames);
    }

    void hashLabelStream(FastBufferedInputStream input, BufferedOutputStream output) throws IOException {
        // Compute intermediate representation and write it on :
        // "<src node id> <dst node id> <label ids>\n"
        ProgressLogger plInter = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plInter.itemsName = "edges";
        plInter.expectedUpdates = graph.numArcs();
        plInter.start("Hashing the label stream");

        var charset = StandardCharsets.US_ASCII;
        byte[] array = new byte[1024];
        for (long line = 0;; line++) {
            int start = 0, len;
            while ((len = input.readLine(array, start, array.length - start,
                    FastBufferedInputStream.ALL_TERMINATORS)) == array.length - start) {
                start += len;
                array = ByteArrays.grow(array, array.length + 1);
            }
            if (len == -1)
                break; // EOF
            final int lineLength = start + len;

            // Skip whitespace at the start of the line.
            int offset = 0;
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            if (offset == lineLength) {
                continue;
            }
            if (array[0] == '#')
                continue;

            // Scan source id.
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            final String ss = new String(array, start, offset - start, charset);

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            if (offset == lineLength) {
                logger.error("Error at line " + line + ": no target");
                continue;
            }

            // Scan target ID
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            final String ts = new String(array, start, offset - start, charset);

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            if (offset == lineLength)
                continue; // No label, skip

            // Scan label
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            final String ls = new String(array, start, offset - start, charset);

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

            // System.err.format("DEBUG: read %s %s %s %d\n", ss, ts, ls, permission);

            long s = swhIdMph.getLong(ss);
            long t = swhIdMph.getLong(ts);
            long filenameId = filenameMph.getLong(ls);

            long srcNode = BigArrays.get(orderMap, s);
            long dstNode = BigArrays.get(orderMap, t);

            output.write((srcNode + "\t" + dstNode + "\t" + filenameId + "\t" + permission + "\n")
                    .getBytes(StandardCharsets.US_ASCII));
            plInter.lightUpdate();
        }
        plInter.done();
    }

    void writeLabels(InputStream sortedMapping) throws IOException {
        // Get the sorted output and write the labels and label offsets
        ProgressLogger plLabels = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plLabels.itemsName = "nodes";
        plLabels.expectedUpdates = graph.numNodes();

        FileWriter debugFile = null;
        if (debugPath != null) {
            debugFile = new FileWriter(debugPath);
        }

        OutputBitStream labels = new OutputBitStream(
                new File(graphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION));
        OutputBitStream offsets = new OutputBitStream(
                new File(graphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION));
        offsets.writeGamma(0);

        Scanner mapLines = new Scanner(sortedMapping, StandardCharsets.US_ASCII);

        NodeIterator it = graph.nodeIterator();
        long labelSrcNode = -1;
        long labelDstNode = -1;
        long labelFilenameId = -1;
        int labelPermission = -1;

        ArrayList<DirEntry> labelBuffer = new ArrayList<>(128);
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            int bits = 0;
            LazyLongIterator s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                while (labelSrcNode <= srcNode && labelDstNode <= dstNode) {
                    if (labelSrcNode == srcNode && labelDstNode == dstNode) {
                        labelBuffer.add(new DirEntry(labelFilenameId, labelPermission));

                        if (debugFile != null) {
                            debugFile.write(labelSrcNode + " " + labelDstNode + " " + labelFilenameId + " "
                                    + labelPermission + "\n");
                        }
                    }

                    if (!mapLines.hasNext())
                        break;

                    String line = mapLines.nextLine();
                    String[] parts = line.split("\\t");
                    labelSrcNode = Long.parseLong(parts[0]);
                    labelDstNode = Long.parseLong(parts[1]);
                    labelFilenameId = Long.parseLong(parts[2]);
                    labelPermission = Integer.parseInt(parts[3]);
                }

                SwhLabel l = new SwhLabel("edgelabel", totalLabelWidth, labelBuffer.toArray(new DirEntry[0]));
                labelBuffer.clear();
                bits += l.toBitStream(labels, -1);
            }
            offsets.writeGamma(bits);
            plLabels.lightUpdate();
        }

        labels.close();
        offsets.close();
        plLabels.done();
        if (debugFile != null) {
            debugFile.close();
        }

        PrintWriter pw = new PrintWriter(new FileWriter((new File(graphPath)).getName() + "-labelled.properties"));
        pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
        pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + SwhLabel.class.getName()
                + "(DirEntry," + totalLabelWidth + ")");
        pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + graphPath);
        pw.close();
    }
}
