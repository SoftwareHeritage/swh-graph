package org.softwareheritage.graph.backend;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;
import com.oath.halodb.HaloDBStats;

import org.softwareheritage.graph.SwhId;

public class NodeIdMapHaloDB {
  private static final int SWH_ID_SIZE = 50;

  HaloDB db;
  String graphPath;

  public NodeIdMapHaloDB(String graphPath, long nbNodes) throws Exception {
    HaloDBOptions options = new HaloDBOptions();
    this.db = null;
    this.graphPath = graphPath;

    // Size of each data file will be 1GB.
    options.setMaxFileSize(1024 * 1024 * 1024);

    // Set the number of threads used to scan index and tombstone files in parallel to build
    // in-memory index during db open. It must be a positive number which is not greater than
    // Runtime.getRuntime().availableProcessors().  It is used to speed up db open time.
    int nbThreads = Runtime.getRuntime().availableProcessors();
    options.setBuildIndexThreads(nbThreads);

    // The threshold at which page cache is synced to disk.  data will be durable only if it is
    // flushed to disk, therefore more data will be lost if this value is set too high. Setting this
    // value too low might interfere with read and write performance.
    options.setFlushDataSizeBytes(10 * 1024 * 1024);

    // The percentage of stale data in a data file at which the file will be compacted.  This value
    // helps control write and space amplification. Increasing this value will reduce write
    // amplification but will increase space amplification.  This along with the compactionJobRate
    // below is the most important setting for tuning HaloDB performance. If this is set to x then
    // write amplification will be approximately 1/x.
    options.setCompactionThresholdPerFile(0.7);

    // Controls how fast the compaction job should run.  This is the amount of data which will be
    // copied by the compaction thread per second.  Optimal value depends on the
    // compactionThresholdPerFile option.
    options.setCompactionJobRate(50 * 1024 * 1024);

    // Setting this value is important as it helps to preallocate enough memory for the off-heap
    // cache. If the value is too low the db might need to rehash the cache. For a db of size n set
    // this value to 2*n.
    long nbRecordsMax = 2 * 2 * nbNodes;
    int nbRecords = (nbRecordsMax > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) nbRecordsMax;
    options.setNumberOfRecords(nbRecords);

    // HaloDB does native memory allocation for the in-memory index.  Enabling this option will
    // release all allocated memory back to the kernel when the db is closed.  This option is not
    // necessary if the JVM is shutdown when the db is closed, as in that case allocated memory is
    // released automatically by the kernel.  If using in-memory index without memory pool this
    // option, depending on the number of records in the database, could be a slow as we need to
    // call _free_ for each record.
    options.setCleanUpInMemoryIndexOnClose(false);

    // ** settings for memory pool **
    options.setUseMemoryPool(true);

    // Hash table implementation in HaloDB is similar to that of ConcurrentHashMap in Java 7.  Hash
    // table is divided into segments and each segment manages its own native memory.  The number of
    // segments is twice the number of cores in the machine.  A segment's memory is further divided
    // into chunks whose size can be configured here.
    options.setMemoryPoolChunkSize(2 * 1024 * 1024);

    // using a memory pool requires us to declare the size of keys in advance.  Any write request
    // with key length greater than the declared value will fail, but it is still possible to store
    // keys smaller than this declared size.
    options.setFixedKeySize(SWH_ID_SIZE);

    db = HaloDB.open("halodb", options);
    dump();

    HaloDBStats stats = db.stats();
    System.out.println(stats.toString());
  }

  public long getNode(SwhId swhId) throws HaloDBException {
    byte[] nodeIdBytes = db.get(swhId.toString().getBytes(StandardCharsets.UTF_8));
    String nodeIdStr = new String(nodeIdBytes, StandardCharsets.UTF_8);
    return Long.parseLong(nodeIdStr);
  }

  public SwhId getSwhId(long node) throws HaloDBException {
    byte[] swhIdBytes = db.get(String.valueOf(node).getBytes(StandardCharsets.UTF_8));
    String swhIdStr = new String(swhIdBytes, StandardCharsets.UTF_8);
    return new SwhId(swhIdStr);
  }

  public void dump() throws Exception {
    // First internal mapping: SWH id (string) -> WebGraph MPH (long)
    @SuppressWarnings("unchecked")
    Object2LongFunction<String> mphMap =
        (Object2LongFunction<String>) BinIO.loadObject(graphPath + ".mph");

    // Second internal mapping: WebGraph MPH (long) -> BFS ordering (long)
    long nbNodes = mphMap.size();
    long[][] bfsMap = LongBigArrays.newBigArray(nbNodes);
    long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
    if (loaded != nbNodes) {
      throw new IllegalArgumentException(
          "Graph contains " + nbNodes + " nodes, but read " + loaded);
    }

    InputStream nodeFile = new GZIPInputStream(new FileInputStream(graphPath + ".nodes.csv.gz"));
    FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(nodeFile, "UTF-8"));
    LineIterator lineIterator = new LineIterator(buffer);

    while (lineIterator.hasNext()) {
      String swhId = lineIterator.next().toString();
      long mphId = mphMap.getLong(swhId);
      long nodeId = LongBigArrays.get(bfsMap, mphId);

      byte[] nodeIdBytes = String.valueOf(nodeId).getBytes(StandardCharsets.UTF_8);
      byte[] swhIdBytes = swhId.getBytes(StandardCharsets.UTF_8);
      db.put(nodeIdBytes, swhIdBytes);
      db.put(swhIdBytes, nodeIdBytes);
    }
  }

  public void close() throws HaloDBException {
    db.close();
  }
}
