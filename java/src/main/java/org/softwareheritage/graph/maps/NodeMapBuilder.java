package org.softwareheritage.graph.maps;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SWHID;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Create maps needed at runtime by the graph service, in particular:
 * <p>
 * <ul>
 * <li>SWHID → WebGraph long node id</li>
 * <li>WebGraph long node id → SWHID (converse of the former)</li>
 * <li>WebGraph long node id → SWH node type (enum)</li>
 * </ul>
 *
 * @author The Software Heritage developers
 */
public class NodeMapBuilder {

    final static String SORT_BUFFER_SIZE = "40%";

    final static Logger logger = LoggerFactory.getLogger(NodeMapBuilder.class);

    /**
     * Main entrypoint.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            logger.error("Usage: COMPRESSED_GRAPH_BASE_NAME TEMP_DIR < NODES_CSV");
            System.exit(1);
        }
        String graphPath = args[0];
        String tmpDir = args[1];

        logger.info("starting maps generation...");
        precomputeNodeIdMap(graphPath, tmpDir);
        logger.info("maps generation completed");
    }

    /**
     * Computes and dumps on disk mapping files.
     *
     * @param graphPath path of the compressed graph
     */
    // Suppress warning for Object2LongFunction cast
    @SuppressWarnings("unchecked")
    static void precomputeNodeIdMap(String graphPath, String tmpDir) throws IOException {
        ProgressLogger plSWHID2Node = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        ProgressLogger plNode2SWHID = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plSWHID2Node.itemsName = "swhid→node";
        plNode2SWHID.itemsName = "node→swhid";

        /*
         * avg speed for swhid→node is sometime skewed due to write to the sort pipe hanging when sort is
         * sorting; hence also desplay local speed
         */
        plSWHID2Node.displayLocalSpeed = true;

        // first half of SWHID->node mapping: SWHID -> WebGraph MPH (long)
        Object2LongFunction<String> mphMap = null;
        try {
            logger.info("loading MPH function...");
            mphMap = (Object2LongFunction<String>) BinIO.loadObject(graphPath + ".mph");
            logger.info("MPH function loaded");
        } catch (ClassNotFoundException e) {
            logger.error("unknown class object in .mph file: " + e);
            System.exit(2);
        }
        long nbIds = (mphMap instanceof Size64) ? ((Size64) mphMap).size64() : mphMap.size();
        plSWHID2Node.expectedUpdates = nbIds;
        plNode2SWHID.expectedUpdates = nbIds;

        // second half of SWHID->node mapping: WebGraph MPH (long) -> BFS order (long)
        long[][] bfsMap = LongBigArrays.newBigArray(nbIds);
        logger.info("loading BFS order file...");
        long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
        logger.info("BFS order file loaded");
        if (loaded != nbIds) {
            logger.error("graph contains " + nbIds + " nodes, but read " + loaded);
            System.exit(2);
        }

        /*
         * Create mapping SWHID -> WebGraph node id, by sequentially reading nodes, hashing them with MPH,
         * and permuting according to BFS order
         */
        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
        LineIterator swhidIterator = new LineIterator(buffer);

        /*
         * The WebGraph node id -> SWHID mapping can be obtained from the SWHID->node one by numerically
         * sorting on node id and sequentially writing obtained SWHIDs to a binary map. Delegates the
         * sorting job to /usr/bin/sort via pipes
         */
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("sort", "--numeric-sort", "--key", "2", "--buffer-size", SORT_BUFFER_SIZE,
                "--temporary-directory", tmpDir);
        Process sort = processBuilder.start();
        BufferedOutputStream sort_stdin = new BufferedOutputStream(sort.getOutputStream());
        BufferedInputStream sort_stdout = new BufferedInputStream(sort.getInputStream());

        // for the binary format of swhidToNodeMap, see Python module swh.graph.swhid:SwhidToIntMap
        // for the binary format of nodeToSwhidMap, see Python module swh.graph.swhid:IntToSwhidMap
        try (DataOutputStream swhidToNodeMap = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(graphPath + Graph.SWHID_TO_NODE)));
                BufferedOutputStream nodeToSwhidMap = new BufferedOutputStream(
                        new FileOutputStream(graphPath + Graph.NODE_TO_SWHID))) {

            /*
             * background handler for sort output, it will be fed SWHID/node pairs while swhidToNodeMap is being
             * filled, and will itself fill nodeToSwhidMap as soon as data from sort is ready
             */
            SortOutputHandler outputHandler = new SortOutputHandler(sort_stdout, nodeToSwhidMap, plNode2SWHID);
            outputHandler.start();

            /*
             * Type map from WebGraph node ID to SWH type. Used at runtime by pure Java graph traversals to
             * efficiently check edge restrictions.
             */
            final int log2NbTypes = (int) Math.ceil(Math.log(Node.Type.values().length) / Math.log(2));
            final int nbBitsPerNodeType = log2NbTypes;
            LongArrayBitVector nodeTypesBitVector = LongArrayBitVector.ofLength(nbBitsPerNodeType * nbIds);
            LongBigList nodeTypesMap = nodeTypesBitVector.asLongBigList(nbBitsPerNodeType);

            plSWHID2Node.start("filling swhid2node map");
            for (long iNode = 0; iNode < nbIds && swhidIterator.hasNext(); iNode++) {
                String swhidStr = swhidIterator.next().toString();
                SWHID swhid = new SWHID(swhidStr);
                byte[] swhidBin = swhid.toBytes();

                long mphId = mphMap.getLong(swhidStr);
                long nodeId = BigArrays.get(bfsMap, mphId);

                swhidToNodeMap.write(swhidBin, 0, swhidBin.length);
                swhidToNodeMap.writeLong(nodeId);
                sort_stdin.write((swhidStr + "\t" + nodeId + "\n").getBytes(StandardCharsets.US_ASCII));

                nodeTypesMap.set(nodeId, swhid.getType().ordinal());
                plSWHID2Node.lightUpdate();
            }
            plSWHID2Node.done();
            sort_stdin.close();

            // write type map
            logger.info("storing type map");
            BinIO.storeObject(nodeTypesMap, graphPath + Graph.NODE_TO_TYPE);
            logger.info("type map stored");

            // wait for nodeToSwhidMap filling
            try {
                logger.info("waiting for node2swhid map...");
                int sortExitCode = sort.waitFor();
                if (sortExitCode != 0) {
                    logger.error("sort returned non-zero exit code: " + sortExitCode);
                    System.exit(2);
                }
                outputHandler.join();
            } catch (InterruptedException e) {
                logger.error("processing of sort output failed with: " + e);
                System.exit(2);
            }
        }

    }

    private static class SortOutputHandler extends Thread {
        private Scanner input;
        private OutputStream output;
        private ProgressLogger pl;

        SortOutputHandler(InputStream input, OutputStream output, ProgressLogger pl) {
            this.input = new Scanner(input, StandardCharsets.US_ASCII);
            this.output = output;
            this.pl = pl;
        }

        public void run() {
            boolean sortDone = false;
            logger.info("node2swhid: waiting for sort output...");
            while (input.hasNextLine()) {
                if (!sortDone) {
                    sortDone = true;
                    this.pl.start("filling node2swhid map");
                }
                String line = input.nextLine(); // format: SWHID <TAB> NODE_ID
                SWHID swhid = new SWHID(line.split("\\t")[0]); // get SWHID
                try {
                    output.write((byte[]) swhid.toBytes());
                } catch (IOException e) {
                    logger.error("writing to node->SWHID map failed with: " + e);
                }
                this.pl.lightUpdate();
            }
            this.pl.done();
        }
    }

}
