package org.softwareheritage.graph.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import it.unimi.dsi.io.ByteBufferOutputStream;

public class MMapOutputFile {
  ByteBufferOutputStream bufferMap;
  int lineLength;

  public MMapOutputFile(String path, int lineLength, long nbLines) throws IOException {
    this.bufferMap = null;
    this.lineLength = lineLength;
    long totalSize = nbLines * lineLength;

    try (RandomAccessFile mapFile = new RandomAccessFile(new File(path), "rw")) {
      FileChannel fileChannel = mapFile.getChannel();
      bufferMap =
          ByteBufferOutputStream.map(fileChannel, totalSize, FileChannel.MapMode.READ_WRITE);
    }
  }

  public void writeAtLine(String line, long lineIndex) {
    long position = lineIndex * (long) lineLength;
    bufferMap.position(position);
    bufferMap.write(line.getBytes(), 0, lineLength);
  }

  public void close() throws IOException {
    bufferMap.close();
  }
}
