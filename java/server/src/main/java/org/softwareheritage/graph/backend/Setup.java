package org.softwareheritage.graph.backend;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
 * Pre-processing steps (such as dumping mapping files on disk) before running the graph service.
 *
 * @author The Software Heritage developers
 */

public class Setup {
    /**
     * Main entrypoint.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: NODES_CSV_GZ COMPRESSED_GRAPH_BASE_NAME");
            System.exit(1);
        }

        String nodesPath = args[0];
        String graphPath = args[1];

        System.out.println("Pre-computing node id maps...");
        long startTime = System.nanoTime();
        precomputeNodeIdMap(nodesPath, graphPath);
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
    static void precomputeNodeIdMap(String nodesPath, String graphPath) throws IOException {
        // First internal mapping: SWH PID (string) -> WebGraph MPH (long)
        Object2LongFunction<String> mphMap = null;
        try {
            mphMap = (Object2LongFunction<String>) BinIO.loadObject(graphPath + ".mph");
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("The .mph file contains unknown class object: " + e);
        }
        long nbIds = (mphMap instanceof Size64) ? ((Size64) mphMap).size64() : mphMap.size();

        // Second internal mapping: WebGraph MPH (long) -> BFS ordering (long)
        long[][] bfsMap = LongBigArrays.newBigArray(nbIds);
        long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
        if (loaded != nbIds) {
            throw new IllegalArgumentException("Graph contains " + nbIds + " nodes, but read " + loaded);
        }

        // Dump complete mapping for all nodes: SWH PID (string) <=> WebGraph node id (long)

        InputStream nodesStream = new GZIPInputStream(new FileInputStream(nodesPath));
        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(nodesStream, "UTF-8"));
        LineIterator swhPIDIterator = new LineIterator(buffer);

	// for the binary format of pidToNodeMap, see Python module swh.graph.pid:PidToIntMap
	// for the binary format of nodeToPidMap, see Python module swh.graph.pid:IntToPidMap
        try (DataOutputStream pidToNodeMap = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(graphPath + Graph.PID_TO_NODE)));
             BufferedOutputStream nodeToPidMap = new BufferedOutputStream(new FileOutputStream(graphPath + Graph.NODE_TO_PID))) {
            // nodeToPidMap needs to write SWH PID in order of node id, so use a temporary array
            Object[][] nodeToSwhPID = ObjectBigArrays.newBigArray(nbIds);

            // To effectively run edge restriction during graph traversals, we store node id (long) -> SWH
            // type map. This is represented as a bitmap using minimum number of bits per Node.Type.
            final int log2NbTypes = (int) Math.ceil(Math.log(Node.Type.values().length) / Math.log(2));
            final int nbBitsPerNodeType = log2NbTypes;
            LongArrayBitVector nodeTypesBitVector =
                LongArrayBitVector.ofLength(nbBitsPerNodeType * nbIds);
            LongBigList nodeTypesMap = nodeTypesBitVector.asLongBigList(nbBitsPerNodeType);

            for (long iNode = 0; iNode < nbIds && swhPIDIterator.hasNext(); iNode++) {
                String strSwhPID = swhPIDIterator.next().toString();
                SwhPID swhPID = new SwhPID(strSwhPID);
		byte[] swhPIDBin = swhPID.toBytes();

                long mphId = mphMap.getLong(strSwhPID);
                long nodeId = LongBigArrays.get(bfsMap, mphId);

		pidToNodeMap.write(swhPIDBin, 0, swhPIDBin.length);
		pidToNodeMap.writeLong(nodeId);

                ObjectBigArrays.set(nodeToSwhPID, nodeId, swhPIDBin);
                nodeTypesMap.set(nodeId, swhPID.getType().ordinal());
            }

            BinIO.storeObject(nodeTypesMap, graphPath + Graph.NODE_TO_TYPE);

            for (long iNode = 0; iNode < nbIds; iNode++) {
                nodeToPidMap.write((byte[]) ObjectBigArrays.get(nodeToSwhPID, iNode));
            }
        }
    }
}
