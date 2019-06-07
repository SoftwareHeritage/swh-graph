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

import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import org.softwareheritage.graph.SwhId;

public class NodeIdMapRocksDB {
  RocksDB db;
  String graphPath;

  public NodeIdMapRocksDB(String graphPath) throws Exception {
    RocksDB.loadLibrary();
    Options rockopts = new Options().setCreateIfMissing(true);
    this.db = null;
    this.graphPath = graphPath;

    // RocksDB tuning

    int nbThreads = Runtime.getRuntime().availableProcessors();
    // To benefit from more threads you might need to set these options to change the max number of
    // concurrent compactions and flushes:
    rockopts.getEnv().setBackgroundThreads(nbThreads, Env.FLUSH_POOL);
    rockopts.getEnv().setBackgroundThreads(nbThreads, Env.COMPACTION_POOL);
    // The default is 1, but to fully utilize your CPU and storage you might want to increase this
    // to approximately number of cores in the system.
    rockopts.setMaxBackgroundCompactions(nbThreads);
    // Set max_open_files to -1 to always keep all files open, which avoids expensive table cache
    // calls.
    rockopts.setMaxOpenFiles(-1);

    db = RocksDB.open(rockopts, "rocksdb");
    dump();
  }

  public long getNode(SwhId swhId) throws RocksDBException {
    byte[] nodeIdBytes = db.get(swhId.toString().getBytes(StandardCharsets.UTF_8));
    String nodeIdStr = new String(nodeIdBytes, StandardCharsets.UTF_8);
    return Long.parseLong(nodeIdStr);
  }

  public SwhId getSwhId(long node) throws RocksDBException {
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

  public void close() {
    db.close();
  }
}
