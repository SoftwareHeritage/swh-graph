/*
 * Copyright (c) 2022-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.shorts.ShortBigArrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.softwareheritage.graph.maps.NodeIdMap;
import org.softwareheritage.graph.compress.ORCGraphDataset.*;
import org.softwareheritage.graph.AllowedNodes;

/**
 * This class is used to extract the node properties from the graph dataset, and write them to a set
 * of property files.
 *
 * Note: because the nodes are not sorted by type, we have an incentive to minimize the number of
 * "holes" in offset arrays. This is why many unrelated properties are cobbled together in the same
 * files (e.g. commit messages, tag messages and origin URLs are all in a "message" property file).
 * Once we migrate to a TypedImmutableGraph as the underlying storage of the graph, we can split all
 * the different properties in their own files.
 */
public class WriteNodeProperties {
    final static Logger logger = LoggerFactory.getLogger(WriteNodeProperties.class);

    private final ORCGraphDataset dataset;
    private final String graphBasename;
    private final NodeIdMap nodeIdMap;
    private final long numNodes;

    public WriteNodeProperties(ORCGraphDataset dataset, String graphBasename, NodeIdMap nodeIdMap) {
        this.dataset = dataset;
        this.graphBasename = graphBasename;
        this.nodeIdMap = nodeIdMap;
        this.numNodes = nodeIdMap.size64();
    }

    public static String[] PROPERTY_WRITERS = new String[]{"timestamps", "content_length", "content_is_skipped",
            "person_ids", "messages", "tag_names",};

    private static JSAPResult parseArgs(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ComposePermutations.class.getName(), "", new Parameter[]{
                    new UnflaggedOption("dataset", JSAP.STRING_PARSER, JSAP.REQUIRED, "Path to the ORC graph dataset"),
                    new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.REQUIRED,
                            "Basename of the output graph"),
                    new FlaggedOption("properties", JSAP.STRING_PARSER, "*", JSAP.NOT_REQUIRED, 'p', "properties",
                            "Properties to write, comma separated (default: all). Possible choices: "
                                    + String.join(",", PROPERTY_WRITERS)),
                    new FlaggedOption("allowedNodeTypes", JSAP.STRING_PARSER, "*", JSAP.NOT_REQUIRED, 'N',
                            "allowed-node-types",
                            "Node types to include in the graph, eg. 'ori,snp,rel,rev' to exclude directories and contents"),});
            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            System.err.println("Usage error: " + e.getMessage());
            System.exit(1);
        }
        return config;
    }

    public static void main(String[] argv) throws IOException, ClassNotFoundException, NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {
        JSAPResult args = parseArgs(argv);
        String datasetPath = args.getString("dataset");
        String graphBasename = args.getString("graphBasename");
        AllowedNodes allowedNodeTypes = new AllowedNodes(args.getString("allowedNodeTypes"));
        NodeIdMap nodeIdMap = new NodeIdMap(graphBasename);

        Set<String> properties;
        if (args.getString("properties").equals("*")) {
            properties = Set.of(PROPERTY_WRITERS);
        } else {
            properties = new HashSet<>(Arrays.asList(args.getString("properties").split(",")));
        }

        ORCGraphDataset dataset = new ORCGraphDataset(datasetPath, allowedNodeTypes);
        WriteNodeProperties writer = new WriteNodeProperties(dataset, graphBasename, nodeIdMap);
        if (properties.contains("timestamps")) {
            writer.writeTimestamps();
        }
        if (properties.contains("content_length")) {
            writer.writeContentLength();
        }
        if (properties.contains("content_is_skipped")) {
            writer.writeContentIsSkipped();
        }
        if (properties.contains("person_ids")) {
            writer.writePersonIds();
        }
        if (properties.contains("messages")) {
            writer.writeMessages();
        }
        if (properties.contains("tag_names")) {
            writer.writeTagNames();
        }
    }

    public void writeContentLength() throws IOException {
        logger.info("Writing content lengths");
        long[][] valueArray = LongBigArrays.newBigArray(numNodes);
        BigArrays.fill(valueArray, -1);

        for (String tableName : new String[]{"content", "skipped_content"}) {
            SwhOrcTable table = dataset.getTable(tableName);
            if (table == null) {
                continue;
            }
            table.readLongColumn("length", (swhid, value) -> {
                long id = nodeIdMap.getNodeId(swhid);
                BigArrays.set(valueArray, id, value);
            });
        }

        BinIO.storeLongs(valueArray, graphBasename + ".property.content.length.bin");
    }

    public void writeContentIsSkipped() throws IOException {
        LongArrayBitVector isSkippedBitVector = LongArrayBitVector.ofLength(numNodes);
        SwhOrcTable table = dataset.getTable("skipped_content");
        if (table != null) {
            table.readIdColumn((swhid) -> {
                long id = nodeIdMap.getNodeId(swhid);
                isSkippedBitVector.set(id);
            });
        }
        BinIO.storeObject(isSkippedBitVector, graphBasename + ".property.content.is_skipped.bin");
    }

    public void writeTimestamps() throws IOException {
        SwhOrcTable releaseTable = dataset.getTable("release");
        SwhOrcTable revisionTable = dataset.getTable("revision");

        if (releaseTable == null && revisionTable == null) {
            return;
        }

        logger.info("Writing author/committer timestamps for release + revision");

        long[][] timestampArray = LongBigArrays.newBigArray(numNodes);
        short[][] timestampOffsetArray = ShortBigArrays.newBigArray(numNodes);

        // Author timestamps
        BigArrays.fill(timestampArray, Long.MIN_VALUE);
        BigArrays.fill(timestampOffsetArray, Short.MIN_VALUE);
        if (releaseTable != null) {
            releaseTable.readTimestampColumn("date", "date_offset", (swhid, date, dateOffset) -> {
                long id = nodeIdMap.getNodeId(swhid);
                BigArrays.set(timestampArray, id, date);
                BigArrays.set(timestampOffsetArray, id, dateOffset);
            });
        }
        if (revisionTable != null) {
            revisionTable.readTimestampColumn("date", "date_offset", (swhid, date, dateOffset) -> {
                long id = nodeIdMap.getNodeId(swhid);
                BigArrays.set(timestampArray, id, date);
                BigArrays.set(timestampOffsetArray, id, dateOffset);
            });
        }
        BinIO.storeLongs(timestampArray, graphBasename + ".property.author_timestamp.bin");
        BinIO.storeShorts(timestampOffsetArray, graphBasename + ".property.author_timestamp_offset.bin");

        // Committer timestamps
        if (revisionTable != null) {
            BigArrays.fill(timestampArray, Long.MIN_VALUE);
            BigArrays.fill(timestampOffsetArray, Short.MIN_VALUE);
            revisionTable.readTimestampColumn("committer_date", "committer_offset", (swhid, date, dateOffset) -> {
                long id = nodeIdMap.getNodeId(swhid);
                BigArrays.set(timestampArray, id, date);
                BigArrays.set(timestampOffsetArray, id, dateOffset);
            });
            BinIO.storeLongs(timestampArray, graphBasename + ".property.committer_timestamp.bin");
            BinIO.storeShorts(timestampOffsetArray, graphBasename + ".property.committer_timestamp_offset.bin");
        }
    }

    public void writePersonIds() throws IOException {
        SwhOrcTable releaseTable = dataset.getTable("release");
        SwhOrcTable revisionTable = dataset.getTable("revision");

        if (releaseTable == null && revisionTable == null) {
            return;
        }

        logger.info("Writing author/committer IDs for release + revision");
        Object2LongFunction<byte[]> personIdMap = NodeIdMap.loadMph(graphBasename + ".persons.mph");
        int[][] personArray = IntBigArrays.newBigArray(numNodes);

        // Author IDs
        BigArrays.fill(personArray, -1);
        if (releaseTable != null) {
            releaseTable.readBytes64Column("author", (swhid, personBase64) -> {
                long id = nodeIdMap.getNodeId(swhid);
                BigArrays.set(personArray, id, (int) personIdMap.getLong(personBase64));
            });
        }
        if (revisionTable != null) {
            revisionTable.readBytes64Column("author", (swhid, personBase64) -> {
                long id = nodeIdMap.getNodeId(swhid);
                BigArrays.set(personArray, id, (int) personIdMap.getLong(personBase64));
            });
        }
        BinIO.storeInts(personArray, graphBasename + ".property.author_id.bin");

        // Committer IDs
        BigArrays.fill(personArray, -1);
        if (revisionTable != null) {
            revisionTable.readBytes64Column("committer", (swhid, personBase64) -> {
                long id = nodeIdMap.getNodeId(swhid);
                BigArrays.set(personArray, id, (int) personIdMap.getLong(personBase64));
            });
        }
        BinIO.storeInts(personArray, graphBasename + ".property.committer_id.bin");
    }

    public void writeMessages() throws IOException {
        SwhOrcTable releaseTable = dataset.getTable("release");
        SwhOrcTable revisionTable = dataset.getTable("revision");
        OriginOrcTable originTable = (OriginOrcTable) dataset.getTable("origin");

        if (releaseTable == null && revisionTable == null && originTable == null) {
            return;
        }

        logger.info("Writing messages for release + revision, and URLs for origins");

        long[][] messageOffsetArray = LongBigArrays.newBigArray(numNodes);
        BigArrays.fill(messageOffsetArray, -1);

        FastBufferedOutputStream messageStream = new FastBufferedOutputStream(
                new FileOutputStream(graphBasename + ".property.message.bin"));
        AtomicLong offset = new AtomicLong(0L);

        if (releaseTable != null) {
            releaseTable.readBytes64Column("message", (swhid, messageBase64) -> {
                long id = nodeIdMap.getNodeId(swhid);
                messageStream.write(messageBase64);
                messageStream.write('\n');
                BigArrays.set(messageOffsetArray, id, offset.longValue());
                offset.addAndGet(messageBase64.length + 1);
            });
        }

        if (revisionTable != null) {
            revisionTable.readBytes64Column("message", (swhid, messageBase64) -> {
                long id = nodeIdMap.getNodeId(swhid);
                messageStream.write(messageBase64);
                messageStream.write('\n');
                BigArrays.set(messageOffsetArray, id, offset.longValue());
                offset.addAndGet(messageBase64.length + 1);
            });
        }

        if (originTable != null) {
            originTable.readURLs((swhid, messageBase64) -> {
                long id = nodeIdMap.getNodeId(swhid);
                messageStream.write(messageBase64);
                messageStream.write('\n');
                BigArrays.set(messageOffsetArray, id, offset.longValue());
                offset.addAndGet(messageBase64.length + 1);
            });
        }

        // TODO: check which one is optimal in terms of memory/disk usage, EF vs mapped file
        BinIO.storeLongs(messageOffsetArray, graphBasename + ".property.message.offset.bin");
        // EliasFanoLongBigList messageOffsetEF = new
        // EliasFanoLongBigList(LongBigArrayBigList.wrap(messageOffsetArray));
        // BinIO.storeObject(messageOffsetEF, graphBasename + ".property.message.offset.bin");
        messageStream.close();
    }

    public void writeTagNames() throws IOException {
        SwhOrcTable releaseTable = dataset.getTable("release");
        if (releaseTable == null) {
            return;
        }

        logger.info("Writing tag names for release");

        long[][] tagNameOffsetArray = LongBigArrays.newBigArray(numNodes);
        BigArrays.fill(tagNameOffsetArray, -1);

        FastBufferedOutputStream tagNameStream = new FastBufferedOutputStream(
                new FileOutputStream(graphBasename + ".property.tag_name.bin"));
        AtomicLong offset = new AtomicLong(0L);

        releaseTable.readBytes64Column("name", (swhid, tagNameBase64) -> {
            long id = nodeIdMap.getNodeId(swhid);
            tagNameStream.write(tagNameBase64);
            tagNameStream.write('\n');
            BigArrays.set(tagNameOffsetArray, id, offset.longValue());
            offset.addAndGet(tagNameBase64.length + 1);
        });

        BinIO.storeLongs(tagNameOffsetArray, graphBasename + ".property.tag_name.offset.bin");
        // EliasFanoLongBigList tagNameOffsetEF = new
        // EliasFanoLongBigList(LongBigArrayBigList.wrap(tagNameOffsetArray));
        // BinIO.storeObject(tagNameOffsetEF, graphBasename + ".property.tag_name.offset.bin");
        tagNameStream.close();
    }
}
