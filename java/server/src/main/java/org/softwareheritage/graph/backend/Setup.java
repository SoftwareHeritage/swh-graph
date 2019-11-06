package org.softwareheritage.graph.backend;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

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
public class Setup {

    final static long PROGRESS_TICK = 1_000_000;
    final static long SORT_BUFFER_SIZE = Runtime.getRuntime().maxMemory() * 40 / 100;  // 40% max_ram

    /**
     * Main entrypoint.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: NODES_CSV_GZ COMPRESSED_GRAPH_BASE_NAME TEMP_DIR");
            System.exit(1);
        }
        String nodesPath = args[0];
        String graphPath = args[1];
        String tmpDir = args[2];

        System.out.println("Pre-computing node id maps...");
        long startTime = System.nanoTime();
        precomputeNodeIdMap(nodesPath, graphPath, tmpDir);
        long endTime = System.nanoTime();
        double duration = (endTime - startTime) / 1_000_000_000;
        System.out.println("Done in: " + duration + " seconds");
    }

    /**
     * Computes and dumps on disk mapping files.
     *
     * @param nodesPath path of the compressed csv nodes file
     * @param graphPath path of the compressed graph
     */
    // Suppress warning for Object2LongFunction cast
    @SuppressWarnings("unchecked")
    static void precomputeNodeIdMap(String nodesPath, String graphPath, String tmpDir)
	throws IOException {
        // first half of PID->node mapping: PID -> WebGraph MPH (long)
        Object2LongFunction<String> mphMap = null;
        try {
            mphMap = (Object2LongFunction<String>) BinIO.loadObject(graphPath + ".mph");
        } catch (ClassNotFoundException e) {
	    System.err.println("unknown class object in .mph file: " + e);
	    System.exit(2);
        }
        long nbIds = (mphMap instanceof Size64) ? ((Size64) mphMap).size64() : mphMap.size();

        // second half of PID->node mapping: WebGraph MPH (long) -> BFS order (long)
        long[][] bfsMap = LongBigArrays.newBigArray(nbIds);
        long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
        if (loaded != nbIds) {
	    System.err.println("graph contains " + nbIds + " nodes, but read " + loaded);
	    System.exit(2);
        }

        // Create mapping SWH PID -> WebGraph node id, by sequentially reading
        // nodes, hasing them with MPH, and permuting according to BFS order
        InputStream nodesStream = new GZIPInputStream(new FileInputStream(nodesPath));
        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(nodesStream,
										 StandardCharsets.US_ASCII));
        LineIterator swhPIDIterator = new LineIterator(buffer);

	// The WebGraph node id -> SWH PID mapping can be obtained from the
	// PID->node one by numerically sorting on node id and sequentially
	// writing obtained PIDs to a binary map. Delegates the sorting job to
	// /usr/bin/sort via pipes
	ProcessBuilder processBuilder = new ProcessBuilder();
	processBuilder.command("sort", "--numeric-sort", "--key", "2",
			       "--buffer-size", Long.toString(SORT_BUFFER_SIZE),
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
	    SortOutputHandler outputHandler = new SortOutputHandler(sort_stdout, nodeToPidMap);
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

            for (long iNode = 0; iNode < nbIds && swhPIDIterator.hasNext(); iNode++) {
		if (iNode > 0 && iNode % PROGRESS_TICK == 0) {
		    System.out.println("pid2node: processed " + iNode / PROGRESS_TICK
				       + "M nodes...");
		}
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
            }
	    sort_stdin.close();

	    // write type map
	    BinIO.storeObject(nodeTypesMap, graphPath + Graph.NODE_TO_TYPE);

	    // wait for nodeToPidMap filling
	    try {
		int sortExitCode = sort.waitFor();
		if (sortExitCode != 0) {
		    System.err.println("sort returned non-zero exit code: " + sortExitCode);
		    System.exit(2);
		}
		outputHandler.join();
	    } catch (InterruptedException e) {
		System.err.println("processing of sort output failed with: " + e);
		System.exit(2);
	    }
        }

    }

    private static class SortOutputHandler extends Thread {
        private Scanner input;
        private OutputStream output;

        SortOutputHandler(InputStream input, OutputStream output) {
            this.input = new Scanner(input, StandardCharsets.US_ASCII);
            this.output = output;
        }

        public void run() {
	    System.out.println("node2pid: waiting for sort output...");
	    long i = -1;
	    while (input.hasNextLine()) {
		i++;
		if (i > 0 && i % PROGRESS_TICK == 0) {
		    System.out.println("node2pid: processed " + i / PROGRESS_TICK
				       + "M nodes...");
		}
		String line = input.nextLine();  // format: SWH_PID <TAB> NODE_ID
		SwhPID swhPID = new SwhPID(line.split("\\t")[0]);  // get PID
		try {
		    output.write((byte[]) swhPID.toBytes());
		} catch (IOException e) {
		    System.err.println("writing to node->PID map failed with: " + e);
		}
	    }
        }
    }

}
