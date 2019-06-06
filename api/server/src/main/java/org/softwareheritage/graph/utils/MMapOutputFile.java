package org.softwareheritage.graph.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import it.unimi.dsi.io.ByteBufferOutputStream;

public class MMapOutputFile {
  ByteBufferOutputStream bufferMap;
  int lineLength;

  public MMapOutputFile(String path, int lineLength, long nbLines) {
    this.bufferMap = null;
    this.lineLength = lineLength;
    long totalSize = nbLines * lineLength;

    try (RandomAccessFile mapFile = new RandomAccessFile(new File(path), "rw")) {
      FileChannel fileChannel = mapFile.getChannel();
      bufferMap =
          ByteBufferOutputStream.map(fileChannel, totalSize, FileChannel.MapMode.READ_WRITE);
    } catch (IOException e) {
      System.out.println("Could not load MMapOutputFile " + path + ": " + e);
    }
  }

  public void writeLine(String line, long lineIndex) {
    long position = lineIndex * (long) lineLength;
    bufferMap.position(position);
    bufferMap.write(line.getBytes(), 0, lineLength);
  }

  public void close() {
    try {
      bufferMap.close();
    } catch (IOException e) {
      System.out.println("Could not close MMapOutputFile: " + e);
    }
  }
}
