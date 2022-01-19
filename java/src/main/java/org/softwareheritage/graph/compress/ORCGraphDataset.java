package org.softwareheritage.graph.compress;

import com.github.luben.zstd.ZstdOutputStream;
import com.google.common.primitives.Bytes;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

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

    private final File datasetDir;

    public ORCGraphDataset(String datasetPath) {
        this(new File(datasetPath));
    }

    public ORCGraphDataset(File datasetDir) {
        if (!datasetDir.exists()) {
            throw new IllegalArgumentException("Dataset " + datasetDir.getName() + " does not exist");
        }
        this.datasetDir = datasetDir;
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
        switch (tableName) {
            case "skipped_content":
                return new SkippedContentOrcTable(tableDir);
            case "content":
                return new ContentOrcTable(tableDir);
            case "directory":
                return new DirectoryOrcTable(tableDir);
            case "directory_entry":
                return new DirectoryEntryOrcTable(tableDir);
            case "revision":
                return new RevisionOrcTable(tableDir);
            case "revision_history":
                return new RevisionHistoryOrcTable(tableDir);
            case "release":
                return new ReleaseOrcTable(tableDir);
            case "snapshot_branch":
                return new SnapshotBranchOrcTable(tableDir);
            case "snapshot":
                return new SnapshotOrcTable(tableDir);
            case "origin_visit_status":
                return new OriginVisitStatusOrcTable(tableDir);
            case "origin_visit":
                return new OriginVisitOrcTable(tableDir);
            case "origin":
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
            return Arrays.copyOfRange(columnVector.vector[row], columnVector.start[row],
                    columnVector.start[row] + columnVector.length[row]);
        }

        interface ReadOrcBatchHandler {
            void accept(VectorizedRowBatch batch, Map<String, Integer> columnMap) throws IOException;
        }

        /**
         * Read the table, calling the given handler for each new batch of rows. Optionally, if columns is
         * not null, will only scan the columns present in this set instead of the entire table.
         */
        public void readOrcTable(ReadOrcBatchHandler batchHandler, Set<String> columns) throws IOException {
            File[] listing = tableDir.listFiles();
            if (listing == null) {
                throw new IOException("No files found in " + tableDir.getName());
            }
            for (File file : listing) {
                readOrcFile(file.getPath(), batchHandler, columns);
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
        protected final ORCTable orcTable;

        protected static final byte[] cntPrefix = "swh:1:cnt:".getBytes();
        protected static byte[] dirPrefix = "swh:1:dir:".getBytes();
        protected static byte[] revPrefix = "swh:1:rev:".getBytes();
        protected static byte[] relPrefix = "swh:1:rel:".getBytes();
        protected static byte[] snpPrefix = "swh:1:snp:".getBytes();
        protected static byte[] oriPrefix = "swh:1:ori:".getBytes();

        public SwhOrcTable(File tableDir) {
            orcTable = new ORCTable(tableDir);
        }

        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            // No nodes or edges to read in the table by default.
        }

        protected static byte[] urlToOriginId(byte[] url) {
            return DigestUtils.sha1Hex(url).getBytes();
        }
    }

    public static class SkippedContentOrcTable extends SwhOrcTable {
        public SkippedContentOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector contentIdVector = (BytesColumnVector) batch.cols[columnMap.get("sha1_git")];
                for (int row = 0; row < batch.size; row++) {
                    byte[] contentId = Bytes.concat(cntPrefix, ORCTable.getBytesRow(contentIdVector, row));
                    nodeCb.onNode(contentId);
                }
            }, Set.of("sha1_git"));
        }
    }

    public static class ContentOrcTable extends SwhOrcTable {
        public ContentOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector contentIdVector = (BytesColumnVector) batch.cols[columnMap.get("sha1_git")];
                for (int row = 0; row < batch.size; row++) {
                    byte[] contentId = Bytes.concat(cntPrefix, ORCTable.getBytesRow(contentIdVector, row));
                    nodeCb.onNode(contentId);
                }
            }, Set.of("sha1_git"));
        }
    }

    public static class DirectoryOrcTable extends SwhOrcTable {
        public DirectoryOrcTable(File tableDir) {
            super(tableDir);
        }

        @Override
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector directoryIdVector = (BytesColumnVector) batch.cols[columnMap.get("id")];
                for (int row = 0; row < batch.size; row++) {
                    byte[] directoryId = Bytes.concat(dirPrefix, ORCTable.getBytesRow(directoryIdVector, row));
                    nodeCb.onNode(directoryId);
                }
            }, Set.of("id"));
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
                    long permission = permissionVector.vector[row];
                    edgeCb.onEdge(src, dst, label, permission);
                }
            }, Set.of("directory_id", "target", "type", "name", "perms"));
        }
    }

    public static class RevisionOrcTable extends SwhOrcTable {
        public RevisionOrcTable(File tableDir) {
            super(tableDir);
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
                    edgeCb.onEdge(parentId, revisionId, null, -1);
                }
            }, Set.of("id", "parent_id"));
        }
    }

    public static class ReleaseOrcTable extends SwhOrcTable {
        public ReleaseOrcTable(File tableDir) {
            super(tableDir);
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
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector snapshotIdVector = (BytesColumnVector) batch.cols[columnMap.get("id")];
                for (int row = 0; row < batch.size; row++) {
                    byte[] snapshotId = Bytes.concat(snpPrefix, ORCTable.getBytesRow(snapshotIdVector, row));
                    nodeCb.onNode(snapshotId);
                }
            }, Set.of("id"));
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
                    if (snapshot_id.length == 0) {
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
        public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
            orcTable.readOrcTable((batch, columnMap) -> {
                BytesColumnVector originUrlVector = (BytesColumnVector) batch.cols[columnMap.get("url")];
                for (int row = 0; row < batch.size; row++) {
                    byte[] originId = Bytes.concat(oriPrefix,
                            urlToOriginId(ORCTable.getBytesRow(originUrlVector, row)));
                    nodeCb.onNode(originId);
                }
            }, Set.of("url"));
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
