package org.softwareheritage.graph.utils;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import org.softwareheritage.graph.maps.NodeIdMap;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class WriteRevisionTimestamps {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        System.err.print("Loading everything...");
        String graphPath = args[0];
        String outputFile = args[1];
        Object2LongFunction<byte[]> mphMap = NodeIdMap.loadMph(graphPath + ".mph");
        long nbIds = (mphMap instanceof Size64) ? ((Size64) mphMap).size64() : mphMap.size();
        long[][] nodePerm = BinIO.loadLongsBig(graphPath + ".order");
        // NodeIdMap nodeIdMap = new NodeIdMap(graphPath, nbIds);
        long[][] timestampArray = LongBigArrays.newBigArray(nbIds);
        BigArrays.fill(timestampArray, Long.MIN_VALUE);
        System.err.println(" done.");

        // TODO: wasteful to convert to/from bytes
        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
        LineIterator lineIterator = new LineIterator(buffer);

        while (lineIterator.hasNext()) {
            String line = lineIterator.next().toString();
            String[] line_elements = line.split("[ \\t]");

            // SWHID currentRev = new SWHID(line_elements[0].strip());
            long revId = -1;
            long timestamp = -1;
            try {
                // revId = nodeIdMap.getNodeId(currentRev);
                long revHash = mphMap.getLong(line_elements[0].strip().getBytes(StandardCharsets.US_ASCII));
                revId = BigArrays.get(nodePerm, revHash);
                timestamp = Long.parseLong(line_elements[1].strip());
            } catch (IllegalArgumentException e) {
                continue;
            }
            BigArrays.set(timestampArray, revId, timestamp);
            // System.err.println(revId + " " + timestamp);
        }
        BinIO.storeLongs(timestampArray, outputFile);
    }
}
