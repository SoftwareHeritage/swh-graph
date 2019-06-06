package org.softwareheritage.graph.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import it.unimi.dsi.io.ByteBufferInputStream;

public class MMapInputFile {
  ByteBufferInputStream bufferMap;
  int lineLength;

  public MMapInputFile(String path, int lineLength) {
    this.bufferMap = null;
    this.lineLength = lineLength;

    try (RandomAccessFile mapFile = new RandomAccessFile(new File(path), "r")) {
      FileChannel fileChannel = mapFile.getChannel();
      bufferMap = ByteBufferInputStream.map(fileChannel, FileChannel.MapMode.READ_ONLY);
    } catch (IOException e) {
      System.out.println("Could not load MMapInputFile " + path + ": " + e);
    }
  }

  public String readLine(long lineIndex) {
    byte[] buffer = new byte[lineLength];
    long position = lineIndex * (long) lineLength;
    bufferMap.position(position);
    bufferMap.read(buffer, 0, lineLength);
    String line = new String(buffer);
    return line.trim();
  }

  public void close() {
    try {
      bufferMap.close();
    } catch (IOException e) {
      System.out.println("Could not close MMapInputFile: " + e);
    }
  }
}
