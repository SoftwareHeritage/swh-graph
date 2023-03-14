/*
 * Copyright (c) 2022-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import com.github.luben.zstd.ZstdInputStream;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.softwareheritage.graph.AllowedNodes;

/**
 * A graph dataset in (zstd-compressed) CSV format.
 *
 * This format does not contain any properties apart from the SWHIDs of the nodes, and optionally
 * the labels of the edges and the permissions of the directory entries.
 *
 * The structure of the dataset is as follows: one directory per object type, each containing:
 *
 * <ul>
 * <li>a number of files <code>*.nodes.csv.zst</code> containing the SWHIDs of the objects stored in
 * the graph, one per line.</li>
 * <li>a number of files <code>*.edges.csv.zst</code> containing the edges of the graph, one per
 * line. The format of each edge is as follows:
 * <code>SRC_SWHID DST_SWHID [BASE64_LABEL] [INT_PERMISSION]</code>.</li>
 * </ul>
 *
 */
public class CSVEdgeDataset implements GraphDataset {
    final static Logger logger = LoggerFactory.getLogger(CSVEdgeDataset.class);

    final private File datasetDir;

    /**
     * Initializes with all node types allowed
     */
    public CSVEdgeDataset(String datasetPath) {
        this(datasetPath, new AllowedNodes("*"));
    }

    public CSVEdgeDataset(String datasetPath, AllowedNodes allowedNodeTypes) {
        this(new File(datasetPath), allowedNodeTypes);
    }

    public CSVEdgeDataset(File datasetDir) {
        this(datasetDir, new AllowedNodes("*"));
    }

    public CSVEdgeDataset(File datasetDir, AllowedNodes allowedNodeTypes) {
        if (allowedNodeTypes.restrictedTo != null) {
            // TODO: Implement support for allowedNodeTypes.
            throw new IllegalArgumentException("allowedNodeTypes must be '*' when using a CSV edge dataset.");
        }
        if (!datasetDir.exists()) {
            throw new IllegalArgumentException("Dataset " + datasetDir.getName() + " does not exist");
        }
        this.datasetDir = datasetDir;
    }

    public void readEdges(GraphDataset.NodeCallback nodeCb, GraphDataset.EdgeCallback edgeCb) throws IOException {
        File[] allTables = datasetDir.listFiles();
        if (allTables == null) {
            return;
        }
        for (File tableFile : allTables) {
            File[] allCsvFiles = tableFile.listFiles();
            if (allCsvFiles == null) {
                continue;
            }
            for (File csvFile : allCsvFiles) {
                if (csvFile.getName().endsWith(".edges.csv.zst")) {
                    readEdgesCsvZst(csvFile.getPath(), edgeCb);
                } else if (csvFile.getName().endsWith(".nodes.csv.zst")) {
                    readNodesCsvZst(csvFile.getPath(), nodeCb);
                }
            }
        }
    }

    public static void readEdgesCsvZst(String csvZstPath, GraphDataset.EdgeCallback cb) throws IOException {
        InputStream csvInputStream = new ZstdInputStream(new BufferedInputStream(new FileInputStream(csvZstPath)));
        readEdgesCsv(csvInputStream, cb);
    }

    public static void readEdgesCsv(InputStream csvInputStream, GraphDataset.EdgeCallback cb) throws IOException {
        FastBufferedInputStream csvReader = new FastBufferedInputStream(csvInputStream);

        Charset charset = StandardCharsets.US_ASCII;
        byte[] array = new byte[1024];
        for (long line = 0;; line++) {
            int start = 0, len;
            while ((len = csvReader.readLine(array, start, array.length - start,
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
            final byte[] ss = Arrays.copyOfRange(array, start, offset);

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
            final byte[] ts = Arrays.copyOfRange(array, start, offset);

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            // Scan label
            byte[] ls = null;
            if (offset < lineLength) {
                start = offset;
                while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                    offset++;
                ls = Arrays.copyOfRange(array, start, offset);
            }

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

            cb.onEdge(ss, ts, ls, permission);
        }
    }

    public static void readNodesCsvZst(String csvZstPath, GraphDataset.NodeCallback cb) throws IOException {
        InputStream csvInputStream = new ZstdInputStream(new BufferedInputStream(new FileInputStream(csvZstPath)));
        readNodesCsv(csvInputStream, cb);
    }

    public static void readNodesCsv(InputStream csvInputStream, GraphDataset.NodeCallback cb) throws IOException {
        FastBufferedInputStream csvReader = new FastBufferedInputStream(csvInputStream);

        byte[] array = new byte[1024];
        for (long line = 0;; line++) {
            int start = 0, len;
            while ((len = csvReader.readLine(array, start, array.length - start,
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
            final byte[] ss = Arrays.copyOfRange(array, start, offset);

            cb.onNode(ss);
        }
    }
}
