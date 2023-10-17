/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import com.github.luben.zstd.ZstdOutputStream;
import com.google.common.primitives.Bytes;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import org.softwareheritage.graph.AllowedNodes;
import org.softwareheritage.graph.SwhType;

/**
 * A graph dataset in ORC format.
 *
 * This format of dataset is a full export of the graph, including all the edge and node properties.
 *
 * For convenience purposes, this class also provides a main method to print all the edges of the
 * graph, so that the output can be piped to
 * {@link it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph}.
 *
 * Reading edges from ORC files using this class is about ~2.5 times slower than reading them
 * directly from a plaintext format.
 */
public class ORCGraphDataset implements GraphDataset {
    final static Logger logger = LoggerFactory.getLogger(ORCGraphDataset.class);

    final static public int ORC_BATCH_SIZE = 16 * 1024;

    private File datasetDir;
    public final AllowedNodes allowedNodeTypes;

    protected ORCGraphDataset() {
        if (!TimeZone.getDefault().getID().equals("UTC")) {
            throw new RuntimeException(
                    "ORCGraphDataset cannot be used in non-UTC timezones (try setting the $TZ environment variable to 'UTC')");
        }
        this.allowedNodeTypes = new AllowedNodes("*");
    }

    /**
     * Initializes with all node types allowed
     */
    public ORCGraphDataset(String datasetPath) {
        this(datasetPath, new AllowedNodes("*"));
    }

    public ORCGraphDataset(String datasetPath, AllowedNodes allowedNodeTypes) {
        this(new File(datasetPath), allowedNodeTypes);
    }

    /**
     * Initializes with all node types allowed
     */
    public ORCGraphDataset(File datasetDir) {
        this(datasetDir, new AllowedNodes("*"));
    }

    public ORCGraphDataset(File datasetDir, AllowedNodes allowedNodeTypes) {
        if (!datasetDir.exists()) {
            throw new IllegalArgumentException("Dataset " + datasetDir.getName() + " does not exist");
        }
        if (!TimeZone.getDefault().getID().equals("UTC")) {
            throw new RuntimeException(
                    "ORCGraphDataset cannot be used in non-UTC timezones (try setting the $TZ environment variable to 'UTC')");
        }
        this.datasetDir = datasetDir;
        this.allowedNodeTypes = allowedNodeTypes;
    }

    /**
     * Return the given table as a {@link SwhOrcTable}. The return value can be down-casted to the type
     * of the specific table it represents.
     */
    public SwhOrcTable getTable(String tableName) {
        File tableDir = new File(datasetDir, tableName);
        if (!tableDir.exists()) {
            return null;
        }
        /*
         * Keep the map between table names and allowedNodeTypes in sync with
         * swh/graph/luigi/compressed_graph.py and swh/dataset/relational.py
         */
        switch (tableName) {
            case "skipped_content":
                if (!allowedNodeTypes.isAllowed(SwhType.CNT)) {
                    return null;
                }
                return new SkippedContentOrcTable(tableDir);
            case "content":
                if (!allowedNodeTypes.isAllowed(SwhType.CNT)) {
                    return null;
                }
                return new ContentOrcTable(tableDir);
            case "directory":
                if (!allowedNodeTypes.isAllowed(SwhType.DIR)) {
                    return null;
                }
                return new DirectoryOrcTable(tableDir);
            case "directory_entry":
                if (!allowedNodeTypes.isAllowed(SwhType.DIR)) {
                    return null;
                }
                return new DirectoryEntryOrcTable(tableDir);
            case "revision":
                if (!allowedNodeTypes.isAllowed(SwhType.REV)) {
                    return null;
                }
                return new RevisionOrcTable(tableDir);
            case "revision_history":
                if (!allowedNodeTypes.isAllowed(SwhType.REV)) {
                    return null;
                }
                return new RevisionHistoryOrcTable(tableDir);
            case "release":
                if (!allowedNodeTypes.isAllowed(SwhType.REL)) {
                    return null;
                }
                return new ReleaseOrcTable(tableDir);
            case "snapshot_branch":
                if (!allowedNodeTypes.isAllowed(SwhType.SNP)) {
                    return null;
                }
                return new SnapshotBranchOrcTable(tableDir);
            case "snapshot":
                if (!allowedNodeTypes.isAllowed(SwhType.SNP)) {
                    return null;
                }
                return new SnapshotOrcTable(tableDir);
            case "origin_visit_status":
                if (!allowedNodeTypes.isAllowed(SwhType.ORI)) {
                    return null;
                }
                return new OriginVisitStatusOrcTable(tableDir);
            case "origin_visit":
                if (!allowedNodeTypes.isAllowed(SwhType.ORI)) {
                    return null;
                }
                return new OriginVisitOrcTable(tableDir);
            case "origin":
                if (!allowedNodeTypes.isAllowed(SwhType.ORI)) {
                    return null;
                }
                return new OriginOrcTable(tableDir);
            default :
                return null;
        }
    }

    /** Return all the tables in this dataset as a map of {@link SwhOrcTable}. */
    public Map<String, SwhOrcTable> allTables() {
        HashMap<String, SwhOrcTable> tables = new HashMap<>();
        File[] tableDirs = datasetDir.listFiles();
        if (tableDirs == null) {
            return tables;
        }
        for (File tableDir : tableDirs) {
            SwhOrcTable table = getTable(tableDir.getName());
            if (table != null) {
                tables.put(tableDir.getName(), table);
            }
        }
        return tables;
    }

    public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
        Map<String, SwhOrcTable> tables = allTables();
        for (SwhOrcTable table : tables.values()) {
            table.readEdges(nodeCb, edgeCb);
        }
    }

    /**
     * A class representing an ORC table, stored on disk as a set of ORC files all in the same
     * directory.
     */
    public static class ORCTable {
        private final File tableDir;

        public ORCTable(File tableDir) {
            if (!tableDir.exists()) {
                throw new IllegalArgumentException("Table " + tableDir.getName() + " does not exist");
            }
            this.tableDir = tableDir;
        }

        public static ORCTable load(File tableDir) {
            return new ORCTable(tableDir);
        }

        /**
         * Utility function for byte columns. Return as a byte array the value of the given row in the
         * column vector.
         */
        public static byte[] getBytesRow(BytesColumnVector columnVector, int row) {
            if (columnVector.isRepeating) {
                row = 0;
            }
            if (columnVector.isNull[row]) {
                return null;
            }
            return Arrays.copyOfRange(columnVector.vector[row], columnVector.start[row],
                    columnVector.start[row] + columnVector.length[row]);
        }

        /**
         * Utility function for long columns. Return as a long the value of the given row in the column
         * vector.
         */
        public static Long getLongRow(LongColumnVector columnVector, int row) {
            if (columnVector.isRepeating) {
                row = 0;
            }
            if (columnVector.isNull[row]) {
                return null;
            }
            return columnVector.vector[row];
        }

        interface ReadOrcBatchHandler {
            void accept(VectorizedRowBatch batch, Map<String, Integer> columnMap) throws IOException;
        }

        /**
         * Read the table, calling the given handler for each new batch of rows. Optionally, if columns is
         * not null, will only scan the columns present in this set instead of the entire table.
         *
         * If this method is called from within a ForkJoinPool, the ORC table will be read in parallel using
         * that thread pool. Otherwise, the ORC files will be read sequentially.
         */
        public void readOrcTable(ReadOrcBatchHandler batchHandler, Set<String> columns) throws IOException {
            File[] listing = tableDir.listFiles();
            if (listing == null) {
                throw new IOException("No files found in " + tableDir.getName());
            }
            ForkJoinPool forkJoinPool = ForkJoinTask.getPool();
            if (forkJoinPool == null) {
                // Sequential case
                for (File file : listing) {
                    readOrcFile(file.getPath(), batchHandler, columns);
                }
            } else {
                // Parallel case
                ArrayList<File> listingArray = new ArrayList<>(Arrays.asList(listing));
                listingArray.parallelStream().forEach(file -> {
                    try {
                        readOrcFile(file.getPath(), batchHandler, columns);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }

        private void readOrcFile(String path, ReadOrcBatchHandler batchHandler, Set<String> columns)
                throws IOException {
            try (Reader reader = OrcFile.createReader(new Path(path), OrcFile.readerOptions(new Configuration()))) {
                TypeDescription schema = reader.getSchema();

                Reader.Options options = reader.options();
                if (columns != null) {
                    options.include(createColumnsToRead(schema, columns));
                }
                Map<String, Integer> columnMap = getColumnMap(schema);

                try (RecordReader records = reader.rows(options)) {
                    VectorizedRowBatch batch = reader.getSchema().createRowBatch(ORC_BATCH_SIZE);
                    while (records.nextBatch(batch)) {
                        batchHandler.accept(batch, columnMap);
                    }
                }
            }
        }

        private static Map<String, Integer> getColumnMap(TypeDescription schema) {
            Map<String, Integer> columnMap = new HashMap<>();
            List<String> fieldNames = schema.getFieldNames();
            for (int i = 0; i < fieldNames.size(); i++) {
                columnMap.put(fieldNames.get(i), i);
            }
            return columnMap;
        }

        private static boolean[] createColumnsToRead(TypeDescription schema, Set<String> columns) {
            boolean[] columnsToRead = new boolean[schema.getMaximumId() + 1];
            List<String> fieldNames = schema.getFieldNames();
            List<TypeDescription> columnTypes = schema.getChildren();
            for (int i = 0; i < fieldNames.size(); i++) {
                if (columns.contains(fieldNames.get(i))) {
                    logger.debug("Adding column " + fieldNames.get(i) + " with ID " + i + " to the read list");
                    TypeDescription type = columnTypes.get(i);
                    for (int id = type.getId(); id <= type.getMaximumId(); id++) {
                        columnsToRead[id] = true;
                    }
                }
            }
            return columnsToRead;
        }
    }

    /** Base class for SWH-specific ORC tables. */
    public static class SwhOrcTable {
        protected ORCTable orcTable;

        protected static final byte[] cntPrefix = "swh:1:cnt:".getBytes();
        protected static final byte[] dirPrefix = "swh:1:dir:".getBytes();
        protected static final byte[] revPrefix = "swh:1:rev:".getBytes();
        protected static final byte[] relPrefix = "swh:1:rel:".getBytes();
        protected static final byte[] snpPrefix = "swh:1:snp:".getBytes();
        protected static final byte[] oriPrefix = "swh:1:ori:".getBytes();

        protected String getIdColumn() {
            return "id";
        }
        protected byte[] getSwhidPrefix() {
            throw new UnsupportedOperationException();
        }
        protected byte[] idToSwhid(byte[] id) {
            return Bytes.concat(getSwhidPrefix(), id);
        }

        protected SwhOrcTable() {
        }

        public SwhOrcTable(File tableDir) {
            orcTable = new ORCTable(tableDir);
        }

        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            // No nodes or edges to read in the table by default.
        }

        protected static byte[] urlToOriginId(byte[] url) {
            return DigestUtils.sha1Hex(url).getBytes();
        }

        public void readIdColumn(NodeCallback cb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector idVector = (BytesColumnVector) batch.cols[columnMap.get(getIdColumn())];

                for (int row = 0; row < batch.size; row++) {
                    byte[] id = idToSwhid(ORCTable.getBytesRow(idVector, row));
                    cb.onNode(id);
                }
            }, Set.of(getIdColumn()));
        }

        public void readLongColumn(String longColumn, LongCallback cb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector idVector = (BytesColumnVector) batch.cols[columnMap.get(getIdColumn())];
                LongColumnVector dateVector = (LongColumnVector) batch.cols[columnMap.get(longColumn)];

                for (int row = 0; row < batch.size; row++) {
                    byte[] id = idToSwhid(ORCTable.getBytesRow(idVector, row));
                    Long date = ORCTable.getLongRow(dateVector, row);
                    if (date != null) {
                        cb.onLong(id, date);
                    }
                }
            }, Set.of(getIdColumn(), longColumn));
        }

        public void readTimestampColumn(String dateColumn, String dateOffsetColumn, TimestampCallback cb)
                throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector idVector = (BytesColumnVector) batch.cols[columnMap.get(getIdColumn())];
                TimestampColumnVector dateVector = (TimestampColumnVector) batch.cols[columnMap.get(dateColumn)];
                LongColumnVector dateOffsetVector = (LongColumnVector) batch.cols[columnMap.get(dateOffsetColumn)];

                for (int row = 0; row < batch.size; row++) {
                    byte[] id = idToSwhid(ORCTable.getBytesRow(idVector, row));
                    long date = dateVector.getTimestampAsLong(row); // rounded to seconds
                    Long dateOffset = ORCTable.getLongRow(dateOffsetVector, row);
                    if (dateOffset != null) {
                        cb.onTimestamp(id, date, dateOffset.shortValue());
                    }
                }
            }, Set.of(getIdColumn(), dateColumn, dateOffsetColumn));
        }

        public void readBytes64Column(String longColumn, BytesCallback cb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector idVector = (BytesColumnVector) batch.cols[columnMap.get(getIdColumn())];
                BytesColumnVector valueVector = (BytesColumnVector) batch.cols[columnMap.get(longColumn)];

                for (int row = 0; row < batch.size; row++) {
                    byte[] id = idToSwhid(ORCTable.getBytesRow(idVector, row));
                    byte[] value = ORCTable.getBytesRow(valueVector, row);
                    if (value != null) {
                        byte[] encodedValue = Base64.getEncoder().encode(value);
                        cb.onBytes(id, encodedValue);
                    }
                }
            }, Set.of(getIdColumn(), longColumn));
        }
    }

    public static class SkippedContentOrcTable extends SwhOrcTable {
        public SkippedContentOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        protected String getIdColumn() {
            return "sha1_git";
        }

        @Override
        protected byte[] getSwhidPrefix() {
            return cntPrefix;
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            readIdColumn(nodeCb);
        }
    }

    public static class ContentOrcTable extends SwhOrcTable {
        public ContentOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        protected String getIdColumn() {
            return "sha1_git";
        }

        @Override
        protected byte[] getSwhidPrefix() {
            return cntPrefix;
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            readIdColumn(nodeCb);
        }
    }

    public static class DirectoryOrcTable extends SwhOrcTable {
        public DirectoryOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        protected byte[] getSwhidPrefix() {
            return dirPrefix;
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            readIdColumn(nodeCb);
        }
    }

    public static class DirectoryEntryOrcTable extends SwhOrcTable {
        public DirectoryEntryOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            byte[] cntType = "file".getBytes();
            byte[] dirType = "dir".getBytes();
            byte[] revType = "rev".getBytes();

            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector srcVector = (BytesColumnVector) batch.cols[columnMap.get("directory_id")];
                BytesColumnVector dstVector = (BytesColumnVector) batch.cols[columnMap.get("target")];
                BytesColumnVector targetTypeVector = (BytesColumnVector) batch.cols[columnMap.get("type")];
                BytesColumnVector labelVector = (BytesColumnVector) batch.cols[columnMap.get("name")];
                LongColumnVector permissionVector = (LongColumnVector) batch.cols[columnMap.get("perms")];

                for (int row = 0; row < batch.size; row++) {
                    byte[] targetType = ORCTable.getBytesRow(targetTypeVector, row);
                    byte[] targetPrefix;
                    if (Arrays.equals(targetType, cntType)) {
                        targetPrefix = cntPrefix;
                    } else if (Arrays.equals(targetType, dirType)) {
                        targetPrefix = dirPrefix;
                    } else if (Arrays.equals(targetType, revType)) {
                        targetPrefix = revPrefix;
                    } else {
                        continue;
                    }

                    byte[] src = Bytes.concat(dirPrefix, ORCTable.getBytesRow(srcVector, row));
                    byte[] dst = Bytes.concat(targetPrefix, ORCTable.getBytesRow(dstVector, row));
                    byte[] label = Base64.getEncoder().encode(ORCTable.getBytesRow(labelVector, row));
                    Long permission = ORCTable.getLongRow(permissionVector, row);
                    edgeCb.onEdge(src, dst, label, permission != null ? permission.intValue() : 0);
                }
            }, Set.of("directory_id", "target", "type", "name", "perms"));
        }
    }

    public static class RevisionOrcTable extends SwhOrcTable {
        public RevisionOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        protected byte[] getSwhidPrefix() {
            return revPrefix;
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector revisionIdVector = (BytesColumnVector) batch.cols[columnMap.get("id")];
                BytesColumnVector directoryIdVector = (BytesColumnVector) batch.cols[columnMap.get("directory")];
                for (int row = 0; row < batch.size; row++) {
                    byte[] revisionId = Bytes.concat(revPrefix, ORCTable.getBytesRow(revisionIdVector, row));
                    byte[] directoryId = Bytes.concat(dirPrefix, ORCTable.getBytesRow(directoryIdVector, row));
                    nodeCb.onNode(revisionId);
                    edgeCb.onEdge(revisionId, directoryId, null, -1);
                }
            }, Set.of("id", "directory"));
        }
    }

    public static class RevisionHistoryOrcTable extends SwhOrcTable {
        public RevisionHistoryOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector revisionIdVector = (BytesColumnVector) batch.cols[columnMap.get("id")];
                BytesColumnVector parentIdVector = (BytesColumnVector) batch.cols[columnMap.get("parent_id")];
                for (int row = 0; row < batch.size; row++) {
                    byte[] parentId = Bytes.concat(revPrefix, ORCTable.getBytesRow(parentIdVector, row));
                    byte[] revisionId = Bytes.concat(revPrefix, ORCTable.getBytesRow(revisionIdVector, row));
                    edgeCb.onEdge(revisionId, parentId, null, -1);
                }
            }, Set.of("id", "parent_id"));
        }
    }

    public static class ReleaseOrcTable extends SwhOrcTable {
        public ReleaseOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        protected byte[] getSwhidPrefix() {
            return relPrefix;
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            byte[] cntType = "content".getBytes();
            byte[] dirType = "directory".getBytes();
            byte[] revType = "revision".getBytes();
            byte[] relType = "release".getBytes();

            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector releaseIdVector = (BytesColumnVector) batch.cols[columnMap.get("id")];
                BytesColumnVector targetIdVector = (BytesColumnVector) batch.cols[columnMap.get("target")];
                BytesColumnVector targetTypeVector = (BytesColumnVector) batch.cols[columnMap.get("target_type")];

                for (int row = 0; row < batch.size; row++) {
                    byte[] targetType = ORCTable.getBytesRow(targetTypeVector, row);

                    byte[] targetPrefix;
                    if (Arrays.equals(targetType, cntType)) {
                        targetPrefix = cntPrefix;
                    } else if (Arrays.equals(targetType, dirType)) {
                        targetPrefix = dirPrefix;
                    } else if (Arrays.equals(targetType, revType)) {
                        targetPrefix = revPrefix;
                    } else if (Arrays.equals(targetType, relType)) {
                        targetPrefix = relPrefix;
                    } else {
                        continue;
                    }

                    byte[] releaseId = Bytes.concat(relPrefix, ORCTable.getBytesRow(releaseIdVector, row));
                    byte[] targetId = Bytes.concat(targetPrefix, ORCTable.getBytesRow(targetIdVector, row));
                    nodeCb.onNode(releaseId);
                    edgeCb.onEdge(releaseId, targetId, null, -1);
                }
            }, Set.of("id", "target", "target_type"));
        }
    }

    public static class SnapshotOrcTable extends SwhOrcTable {
        public SnapshotOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        protected byte[] getSwhidPrefix() {
            return snpPrefix;
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            readIdColumn(nodeCb);
        }
    }

    public static class SnapshotBranchOrcTable extends SwhOrcTable {
        public SnapshotBranchOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            byte[] cntType = "content".getBytes();
            byte[] dirType = "directory".getBytes();
            byte[] revType = "revision".getBytes();
            byte[] relType = "release".getBytes();

            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector snapshotIdVector = (BytesColumnVector) batch.cols[columnMap.get("snapshot_id")];
                BytesColumnVector targetIdVector = (BytesColumnVector) batch.cols[columnMap.get("target")];
                BytesColumnVector targetTypeVector = (BytesColumnVector) batch.cols[columnMap.get("target_type")];
                BytesColumnVector branchNameVector = (BytesColumnVector) batch.cols[columnMap.get("name")];

                for (int row = 0; row < batch.size; row++) {
                    byte[] targetType = ORCTable.getBytesRow(targetTypeVector, row);
                    byte[] targetPrefix;
                    if (Arrays.equals(targetType, cntType)) {
                        targetPrefix = cntPrefix;
                    } else if (Arrays.equals(targetType, dirType)) {
                        targetPrefix = dirPrefix;
                    } else if (Arrays.equals(targetType, revType)) {
                        targetPrefix = revPrefix;
                    } else if (Arrays.equals(targetType, relType)) {
                        targetPrefix = relPrefix;
                    } else {
                        continue;
                    }

                    byte[] snapshotId = Bytes.concat(snpPrefix, ORCTable.getBytesRow(snapshotIdVector, row));
                    byte[] targetId = Bytes.concat(targetPrefix, ORCTable.getBytesRow(targetIdVector, row));
                    byte[] branchName = Base64.getEncoder().encode(ORCTable.getBytesRow(branchNameVector, row));
                    nodeCb.onNode(snapshotId);
                    edgeCb.onEdge(snapshotId, targetId, branchName, -1);
                }
            }, Set.of("snapshot_id", "name", "target", "target_type"));
        }
    }

    public static class OriginVisitStatusOrcTable extends SwhOrcTable {
        public OriginVisitStatusOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector originUrlVector = (BytesColumnVector) batch.cols[columnMap.get("origin")];
                BytesColumnVector snapshotIdVector = (BytesColumnVector) batch.cols[columnMap.get("snapshot")];

                for (int row = 0; row < batch.size; row++) {
                    byte[] originId = urlToOriginId(ORCTable.getBytesRow(originUrlVector, row));
                    byte[] snapshot_id = ORCTable.getBytesRow(snapshotIdVector, row);
                    if (snapshot_id == null || snapshot_id.length == 0) {
                        continue;
                    }
                    edgeCb.onEdge(Bytes.concat(oriPrefix, originId), Bytes.concat(snpPrefix, snapshot_id), null, -1);
                }
            }, Set.of("origin", "snapshot"));
        }
    }

    public static class OriginVisitOrcTable extends SwhOrcTable {
        public OriginVisitOrcTable(File tableDir) {
            super(tableDir);
        }
    }

    public static class OriginOrcTable extends SwhOrcTable {
        public OriginOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        protected byte[] getSwhidPrefix() {
            return oriPrefix;
        }

        @Override
        protected byte[] idToSwhid(byte[] id) {
            return Bytes.concat(getSwhidPrefix(), urlToOriginId(id));
        }

        @Override
        protected String getIdColumn() {
            return "url";
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            readIdColumn(nodeCb);
        }

        public void readURLs(BytesCallback cb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector urlVector = (BytesColumnVector) batch.cols[columnMap.get(getIdColumn())];

                for (int row = 0; row < batch.size; row++) {
                    byte[] id = idToSwhid(ORCTable.getBytesRow(urlVector, row));
                    byte[] url = Base64.getEncoder().encode(ORCTable.getBytesRow(urlVector, row));
                    cb.onBytes(id, url);
                }
            }, Set.of(getIdColumn()));
        }
    }

    /**
     * Export an ORC graph to the CSV edge dataset format as two different files,
     * <code>nodes.csv.zst</code> and <code>edges.csv.zst</code>.
     */
    public static void exportToCsvDataset(String orcDataset, String csvDatasetBasename) throws IOException {
        ORCGraphDataset dataset = new ORCGraphDataset(orcDataset);
        File nodesFile = new File(csvDatasetBasename + ".nodes.csv.zst");
        File edgesFile = new File(csvDatasetBasename + ".edges.csv.zst");
        FastBufferedOutputStream nodesOut = new FastBufferedOutputStream(
                new ZstdOutputStream(new FileOutputStream(nodesFile)));
        FastBufferedOutputStream edgesOut = new FastBufferedOutputStream(
                new ZstdOutputStream(new FileOutputStream(edgesFile)));
        dataset.readEdges((node) -> {
            nodesOut.write(node);
            nodesOut.write('\n');
        }, (src, dst, label, perms) -> {
            edgesOut.write(src);
            edgesOut.write(' ');
            edgesOut.write(dst);
            if (label != null) {
                edgesOut.write(' ');
                edgesOut.write(label);
                edgesOut.write(' ');
            }
            if (perms != -1) {
                edgesOut.write(' ');
                edgesOut.write(Long.toString(perms).getBytes());
            }
            edgesOut.write('\n');
        });
    }

    /**
     * Print all the edges of the graph to stdout. Can be piped to
     * {@link it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph} to import the graph dataset and convert
     * it to a {@link it.unimi.dsi.big.webgraph.BVGraph}.
     */
    public static void printSimpleEdges(String orcDataset) throws IOException {
        ORCGraphDataset dataset = new ORCGraphDataset(orcDataset);
        FastBufferedOutputStream out = new FastBufferedOutputStream(System.out);
        dataset.readEdges((node) -> {
        }, (src, dst, label, perms) -> {
            out.write(src);
            out.write(' ');
            out.write(dst);
            out.write('\n');
        });
        out.flush();
    }

    public static void main(String[] args) throws IOException {
        printSimpleEdges(args[0]);
    }
}
