package org.softwareheritage.graph.backend;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import it.unimi.dsi.io.ByteBufferInputStream;

/**
 * Wrapper class around very big mmap()-ed file.
 * <p>
 * Java has a limit for mmap()-ed files because of unsupported 64-bit indexing. The <a
 * href="http://dsiutils.di.unimi.it/">dsiutils</a> ByteBufferInputStream is used to overcome this
 * Java limit.
 *
 * @author The Software Heritage developers
 */

public class MapFile {
    /** Memory-mapped file buffer */
    ByteBufferInputStream bufferMap;
    /** Fixed line length of the mmap()-ed file */
    int lineLength;

    /**
     * Constructor.
     *
     * @param path file path to mmap()
     * @param lineLength fixed length of a line in the file
     */
    public MapFile(String path, int lineLength) throws IOException {
        this.bufferMap = null;
        this.lineLength = lineLength;

        try (RandomAccessFile mapFile = new RandomAccessFile(new File(path), "r")) {
            FileChannel fileChannel = mapFile.getChannel();
            bufferMap = ByteBufferInputStream.map(fileChannel, FileChannel.MapMode.READ_ONLY);
        }
    }

    /**
     * Returns a specific line in the file.
     *
     * @param lineIndex line number in the file
     * @return the line at the specified position
     */
    public String readAtLine(long lineIndex) {
        byte[] buffer = new byte[lineLength];
        long position = lineIndex * (long) lineLength;
        bufferMap.position(position);
        bufferMap.read(buffer, 0, lineLength);
        String line = new String(buffer);
        return line.trim();
    }

    /**
     * Closes the mmap()-ed file.
     */
    public void close() throws IOException {
        bufferMap.close();
    }
}
