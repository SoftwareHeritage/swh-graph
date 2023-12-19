package org.softwareheritage.graph.utils;

import java.util.*;
import java.io.IOException;

// Like {@class StringBuffer}, but can grow over 2^31 bytes.
class BigStringBuffer implements Appendable {
    Vector<StringBuffer> buffers;
    final long MAX_BYTES_PER_CHAR = 4;

    BigStringBuffer(int capacity) {
        buffers = new Vector<StringBuffer>();
        buffers.add(new StringBuffer(capacity));
    }

    void ensureCanAppend(int length) {
        if ((((long) buffers.lastElement().length()) + ((long) length) + 3L) * MAX_BYTES_PER_CHAR
                * 2 > ((long) Integer.MAX_VALUE)) {
            // Why /2? Who knows. Without it, we get
            // "java.lang.OutOfMemoryError: UTF16 String size is 2147483643, should be less than 1073741823"
            // and
            // "append3 OOMed. buffers.length()==2, buffers.lastElement().length()==10072857, start=0, end=15:
            // java.lang.OutOfMemoryError: UTF16 String size is 2147483643, should be less than 1073741823"
            buffers.add(new StringBuffer(Integer.MAX_VALUE / 2 - 4));
        }
    }

    public BigStringBuffer append(char c) {
        ensureCanAppend(1);
        try {
            buffers.lastElement().append(c);
        } catch (OutOfMemoryError e) {
            System.err.format("append1 OOMed. buffers.length()==%d, buffers.lastElement().size()==%d: %s\n",
                    buffers.size(), buffers.lastElement().length(), e);
            throw new RuntimeException(e);
        }
        return this;
    }

    public BigStringBuffer append(CharSequence csq) {
        ensureCanAppend(csq.length());
        try {
            buffers.lastElement().append(csq);
        } catch (OutOfMemoryError e) {
            System.err.format(
                    "append2 OOMed. buffers.length()==%d, buffers.lastElement().length()==%d, csq.length()==%d: %s\n",
                    buffers.size(), buffers.lastElement().length(), csq.length(), e);
            throw new RuntimeException(e);
        }
        return this;
    }

    public BigStringBuffer append(CharSequence csq, int start, int end) {
        ensureCanAppend(end - start);
        try {
            buffers.lastElement().append(csq, start, end);
        } catch (OutOfMemoryError e) {
            System.err.format(
                    "append3 OOMed. buffers.length()==%d, buffers.lastElement().length()==%d, start=%d, end=%d: %s\n",
                    buffers.size(), buffers.lastElement().length(), start, end, e);
            throw new RuntimeException(e);
        }
        return this;
    }

    long length() {
        long r = 0;
        for (StringBuffer buffer : buffers) {
            r += buffer.length();
        }
        return r;
    }

    /*
     * Writes the content of the buffer to stdout, in a <code>synchronized (System.out)</code> block
     */
    void flushToStdout() throws IOException {
        Vector<byte[]> arrays = new Vector<byte[]>(buffers.size());
        for (StringBuffer buffer : buffers) {
            arrays.add(buffer.toString().getBytes());
        }
        synchronized (System.out) {
            for (byte[] array : arrays) {
                System.out.write(array);
            }
        }
    }
}
