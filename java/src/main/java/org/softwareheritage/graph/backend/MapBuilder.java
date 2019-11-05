package org.softwareheritage.graph.backend;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.*;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.logging.ProgressLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.backend.NodeTypesMap;

/**
 * Create maps needed at runtime by the graph service, in particular:
 *
 * - SWH PID → WebGraph long node id
 * - WebGraph long node id → SWH PID (converse of the former)
 * - WebGraph long node id → SWH node type (enum)
 *
 * @author The Software Heritage developers
 */
public class MapBuilder {

    final static String SORT_BUFFER_SIZE = "40%";

    final static Logger logger = LoggerFactory.getLogger(MapBuilder.class);

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
    static void precomputeNodeIdMap(String graphPath, String tmpDir)
        throws IOException
    {
        ProgressLogger plPid2Node = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        ProgressLogger plNode2Pid = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plPid2Node.itemsName = "pid→node";
        plNode2Pid.itemsName = "node→pid";

        // avg speed for pid→node is sometime skewed due to write to the sort
        // pipe hanging when sort is sorting; hence also desplay local speed
        plPid2Node.displayLocalSpeed = true;

        // first half of PID->node mapping: PID -> WebGraph MPH (long)
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
        plPid2Node.expectedUpdates = nbIds;
        plNode2Pid.expectedUpdates = nbIds;

        // second half of PID->node mapping: WebGraph MPH (long) -> BFS order (long)
        long[][] bfsMap = LongBigArrays.newBigArray(nbIds);
        logger.info("loading BFS order file...");
        long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
        logger.info("BFS order file loaded");
        if (loaded != nbIds) {
            logger.error("graph contains " + nbIds + " nodes, but read " + loaded);
            System.exit(2);
        }

        // Create mapping SWH PID -> WebGraph node id, by sequentially reading
        // nodes, hashing them with MPH, and permuting according to BFS order
        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in,
                                                                                 StandardCharsets.US_ASCII));
        LineIterator swhPIDIterator = new LineIterator(buffer);

        // The WebGraph node id -> SWH PID mapping can be obtained from the
        // PID->node one by numerically sorting on node id and sequentially
        // writing obtained PIDs to a binary map. Delegates the sorting job to
        // /usr/bin/sort via pipes
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("sort", "--numeric-sort", "--key", "2",
                               "--buffer-size", SORT_BUFFER_SIZE,
                               "--temporary-directory", tmpDir);
        Process sort = processBuilder.start();
        BufferedOutputStream sort_stdin = new BufferedOutputStream(sort.getOutputStream());
        BufferedInputStream sort_stdout = new BufferedInputStream(sort.getInputStream());

        // for the binary format of pidToNodeMap, see Python module swh.graph.pid:PidToIntMap
        // for the binary format of nodeToPidMap, see Python module swh.graph.pid:IntToPidMap
        try (DataOutputStream pidToNodeMap = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(graphPath + Graph.PID_TO_NODE)));
             BufferedOutputStream nodeToPidMap = new BufferedOutputStream(new FileOutputStream(graphPath + Graph.NODE_TO_PID))) {

            // background handler for sort output, it will be fed PID/node
            // pairs while pidToNodeMap is being filled, and will itself fill
            // nodeToPidMap as soon as data from sort is ready
            SortOutputHandler outputHandler = new SortOutputHandler(sort_stdout, nodeToPidMap, plNode2Pid);
            outputHandler.start();

            // Type map from WebGraph node ID to SWH type. Used at runtime by
            // pure Java graph traversals to efficiently check edge
            // restrictions.
            final int log2NbTypes = (int) Math.ceil(Math.log(Node.Type.values().length)
                                                    / Math.log(2));
            final int nbBitsPerNodeType = log2NbTypes;
            LongArrayBitVector nodeTypesBitVector =
                LongArrayBitVector.ofLength(nbBitsPerNodeType * nbIds);
            LongBigList nodeTypesMap = nodeTypesBitVector.asLongBigList(nbBitsPerNodeType);

            plPid2Node.start("filling pid2node map");
            for (long iNode = 0; iNode < nbIds && swhPIDIterator.hasNext(); iNode++) {
                String strSwhPID = swhPIDIterator.next().toString();
                SwhPID swhPID = new SwhPID(strSwhPID);
                byte[] swhPIDBin = swhPID.toBytes();

                long mphId = mphMap.getLong(strSwhPID);
                long nodeId = LongBigArrays.get(bfsMap, mphId);

                pidToNodeMap.write(swhPIDBin, 0, swhPIDBin.length);
                pidToNodeMap.writeLong(nodeId);
                sort_stdin.write((strSwhPID + "\t" + nodeId + "\n")
                                 .getBytes(StandardCharsets.US_ASCII));

                nodeTypesMap.set(nodeId, swhPID.getType().ordinal());
                plPid2Node.lightUpdate();
            }
            plPid2Node.done();
            sort_stdin.close();

            // write type map
            logger.info("storing type map");
            BinIO.storeObject(nodeTypesMap, graphPath + Graph.NODE_TO_TYPE);
            logger.info("type map stored");

            // wait for nodeToPidMap filling
            try {
                logger.info("waiting for node2pid map...");
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
            logger.info("node2pid: waiting for sort output...");
            while (input.hasNextLine()) {
                if (! sortDone) {
                    sortDone = true;
                    this.pl.start("filling node2pid map");
                }
                String line = input.nextLine();  // format: SWH_PID <TAB> NODE_ID
                SwhPID swhPID = new SwhPID(line.split("\\t")[0]);  // get PID
                try {
                    output.write((byte[]) swhPID.toBytes());
                } catch (IOException e) {
                    logger.error("writing to node->PID map failed with: " + e);
                }
                this.pl.lightUpdate();
            }
            this.pl.done();
        }
    }

}
