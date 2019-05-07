package it.unimi.dsi.law.warc.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Copyright (C) 2004-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.law.warc.util.Util;


// RELEASE-STATUS: DIST

/** An input stream that wraps an underlying input stream to make it
 *  rewindable and partially inspectable,  using a bounded-capacity memory buffer and an overflow file.
 *
 *  <h2>Stream behaviour</h2>
 *
 *  <p>In the following description, we let <var>K</var><sub>0</sub> be the buffer
 *  size, <var>K</var> be the number of bytes read from the underlying stream,
 *  <var>P</var> be the index of the next byte that will be returned by this
 *  stream (indices start from 0) and <var>L</var> be the number of bytes that
 *  will be required. Note that <var>P</var>&le;<var>K</var>, and equality
 *  holds if this stream was never rewound; otherwise, <var>P</var> may be
 *  smaller than <var>K</var> (and, in particular, it will be zero just after
 *  a rewind).
 *
 *  <p>When the stream is connected, up to <var>K</var><sub>0</sub> bytes are read
 *  and stored in the buffer; after that, the buffer itself becomes available for
 *  inspection. Of course, <var>K</var> is set to the number of bytes actually read,
 *  whereas <var>P</var>=0.
 *
 *  <p>Upon reading, as long as <var>P</var>+<var>L</var>-1&lt;<var>K</var>, no byte must actually be read from the input
 *  stream. Otherwise, up to <var>P</var>+<var>L</var>-<var>K</var> bytes are read from the input
 *  stream and stored onto the overflow file before returning them to the user.
 *
 *  <h2>Connecting and disposing</h2>
 *
 *  <p>Objects of this class are reusable by design. At any moment, they may be in one of three states:
 *  connected, ready, disposed:
 *  <ul>
 *  	<li> <strong>connected</strong>: this stream is connected to an underlying input stream, and has an
 *  overflow file open and partially filled; notice that, since the overflow file is reused, the file itself
 *  may be larger than the number of bytes written in it;
 *  	<li> <strong>ready</strong>: this stream is not connected to any underlying input stream, but it has
 *  an overflow file (not open, but ready to be used); notice that, since the overflow file is reused, the file itself
 *  may be nonempty;
 *  	<li> <strong>disposed</strong>: this stream cannot be used anymore: its resources are disposed and, in particular,
 *  its overflow file was actually deleted.
 *  </ul>
 *
 *  <p>At creation, this stream is ready; it can be connected using {@link #connect(InputStream)}. At any time,
 *  it can become ready again by a call to {@link #close()}. The {@link #close()} method does not truncate the
 *  overflow file; if the user wants to truncate the file, it can do so by calling {@link #truncate(long)} after
 *  closing. The {@link #dispose()} method makes this stream disposed; this method is called on finalization.
 *
 *  <h2>Buffering</h2>
 *
 *  <p>This class provides no form of buffering except for the memory buffer described above. Users should consider providing
 *  a buffered underlying input stream, or wrapping instances of this class by a {@link FastBufferedInputStream}: the
 *  former would be appropriate only for those cases when {@link #fillAndRewind()} is not used; the latter can make accesses more efficient,
 *  only if the size of the underlying input stream is often much larger than the buffer size.
 *
 */
public class InspectableBufferedInputStream extends MeasurableInputStream {

	public static final Logger LOGGER = LoggerFactory.getLogger(InspectableBufferedInputStream.class);
	public static final boolean DEBUG = false;

	/** The number of path elements for the hierarchical overflow file (see {@link Util#createHierarchicalTempFile(File, int, String, String)}). */
	public static final int OVERFLOW_FILE_RANDOM_PATH_ELEMENTS = 3;

	/** The possible states of this stream, as explained above. */
	public static enum State { CONNECTED, READY, DISPOSED };

	/** The default buffer size (64KiB). */
	public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

	/** A private throw-away buffer used by {@link #fill(long)} and {@link #skip(long)}. */
	private final byte[] b = new byte[8 * 1024];

	/** The buffer. When connected, it is filled with the first portion of the underlying input stream (read at connection).
	 *  The buffer is available for inspection, but users should not modify its content; the number of bytes actually available
	 *  is {@link #inspectable}.
	 */
	public byte[] buffer;

	/** Whether we already got a -1 from a read operation. */
	boolean eof;

	/** The number of bytes read in the buffer, when connected. It is the minimum between <code>buffer.size</code> and the length
	 *  of the underlying stream.
	 */
	public int inspectable;

	/** The overflow file used by this stream: it is created at construction time, and deleted on {@link #dispose()}, finalization,
	 *  or exit.
	 */
	public final File overflowFile;

	/** When connected, this is the output stream of the overflow file where data should be written. */
	private FileOutputStream overflowOut;

	/** When connected, this is the file channel that underlies the output stream of the overflow file. */
	private FileChannel overflowOutChannel;

	/** When connected, this is the input stream of the overflow file whence the data should be read. This is
	 *  positioned at the beginning of the input stream if <code>position</code>&lt;<code>buffer.length</code>;
	 *  otherwise, the next byte returned by this stream is going to be the (<code>position</code>-<code>buffer.length</code>)-th
	 *  of this stream (numbered from 0). It is anyway always positioned before <code>overflowOut</code>. */
	private FileInputStream overflowIn;

	/** When connected, this is the file channel that underlies the input stream of the overflow file. */
	private FileChannel overflowInChannel;

	/** When connected, this is the underlying input stream. */
	private InputStream underlying;

	/** When connected, this is the number of bytes (ever) read from {@link #underlying}. */
	private long readBytes;

	/** The position on this stream (i.e., the index of the next byte to be returned). */
	private long position;

	/** The state of this stream. */
	private State state;

	/** Whether we know the length already (this only happens if the entire underlying file has been completely read). */
	private boolean lengthKnown;

	/** Creates a new ready stream.
	 *
	 * @param bufferSize the buffer size, in bytes.
	 * @param overflowFileDir the directory where the overflow file should be created, or {@code null} for the default temporary directory.
	 * @throws IOException if some exception occurs during creation.
	 */
	public InspectableBufferedInputStream(final int bufferSize, File overflowFileDir) throws IOException {
		if (overflowFileDir != null && ! overflowFileDir.isDirectory()) throw new IllegalArgumentException("Wrong overflow directory " + overflowFileDir);
		if (bufferSize <=0) throw new IllegalArgumentException("Wrong buffer size " + bufferSize);
		buffer = new byte[bufferSize];
		if (overflowFileDir == null) overflowFileDir = new File(System.getProperty("java.io.tmpdir", "/tmp"));
		overflowFile = Util.createHierarchicalTempFile(overflowFileDir, OVERFLOW_FILE_RANDOM_PATH_ELEMENTS, getClass().getSimpleName() + '.', ".overflow");
		LOGGER.debug("Creating overflow file " + overflowFile);
		overflowFile.deleteOnExit();
		state = State.READY;

		overflowOut = new FileOutputStream(overflowFile);
		overflowOutChannel = overflowOut.getChannel();
		overflowIn = new FileInputStream(overflowFile);
		overflowInChannel = overflowIn.getChannel();
	}

	/**  Creates a new ready stream using default temporary directory for the overflow file.
	 *
	 * @param bufferSize the buffer size, in bytes.
	 * @throws IOException if some exception occurs during creation.
	 */
	public InspectableBufferedInputStream(final int bufferSize) throws IOException {
		this(bufferSize, null);
	}

	/**  Creates a new ready stream with default buffer size, and using default temporary directory for the overflow file.
	 *
	 * @throws IOException if some exception occurs during creation.
	 */
	public InspectableBufferedInputStream() throws IOException {
		this(DEFAULT_BUFFER_SIZE);
	}

	/** Connects to a given input stream, and fills the buffer accordingly. Can only be called on a non-disposed stream.
	 *
	 * @param underlying the underlying input stream to which we should connect.
	 * @throws IOException if some exception occurs while reading
	 */
	public void connect(final InputStream underlying) throws IOException {
		if (state == State.DISPOSED) throw new IllegalStateException("Connecting a disposed stream");
		if (underlying == null) throw new IllegalArgumentException("Cannot connect to null");
		this.underlying = underlying;

		inspectable = 0;
		eof = false;
		int result;
		while((result = underlying.read(buffer, inspectable, buffer.length - inspectable)) > 0) inspectable += result;
		if (result < 0) eof = true;
		readBytes = inspectable;
		position = 0;

		overflowInChannel.position(0);
		overflowOutChannel.position(0);
		state = State.CONNECTED;
		lengthKnown = false;
	}

	/** Truncates the overflow file to a given size. Can only be called when this stream is ready.
	 *
	 * @param size the new size; the final size is guaranteed to be no more than this.
	 * @throws IOException if some exception occurs while truncating the file
	 */
	public void truncate(final long size) throws FileNotFoundException, IOException {
		if (state != State.READY) throw new IllegalStateException("Truncation is possible only for non-connected and non-disposed streams");
		overflowOutChannel.truncate(size);
	}

	/** The number of bytes read so far from the underlying stream.
	 *
	 * @return the number of bytes read so far from the underlying stream.
	 */
	public long readBytes() {
		return readBytes;
	}

	/** Disposes this stream, deleting the overflow file and nulling the buffer. After this, the stream is unusable. */
	public void dispose() throws IOException {
		buffer = null;
		overflowOut.close();
		overflowIn.close();
		overflowFile.delete();
		state = State.DISPOSED;
	}

	protected void finalize() throws Throwable {
		try {
			if (state != State.DISPOSED) dispose();
		}
		finally {
			super.finalize();
		}
	}

	/** Makes this stream ready. Can only be called on a non-disposed stream. If the stream is ready, it does nothing. If the stream
	 *  is connected, it closes the underlying stream, making this stream ready for a new {@link #connect(InputStream) connection} or to be
	 *  {@link #dispose() disposed}.
	 *
	 */
	public void close() throws IOException {
		if (state == State.READY) return;
		if (state == State.DISPOSED) throw new IllegalStateException("Cannot close a disposed s tream");
		underlying.close();
		readBytes = position = 0;
		state = State.READY;
	}

	/** Rewinds this stream. Can only be called on a connected stream. */
	public void rewind() throws IOException {
		if (state != State.CONNECTED) throw new IllegalStateException("Cannot rewind a non-connected (" + state + ") stream");
		position = 0;
		overflowInChannel.position(0);
	}


	@Override
	public int available() throws IOException {
		if (state != State.CONNECTED) throw new IllegalStateException();
		long av = readBytes - position;
		if (! eof) av += underlying.available();
		return (int)Math.min(Integer.MAX_VALUE, av);
	}

	/** Reads at most <code>k</code> bytes from the underlying stream, using the given buffer and starting
	 *  from a given offset, and copy them to the overflow file. Updates the number of bytes read. Differently from all other
	 *  public methods, this method does not perform any state-consistency check.
	 *
	 * @param buffer the buffer where the bytes are read.
	 * @param offset the offset from where the bytes are written onto <code>buffer</code>.
	 * @param length the maximum number of bytes to be read.
	 * @return the number of bytes actually read.
	 * @throws IOException if some exception occurs while copying.
	 */
	private int copy(byte[] buffer, int offset, int length) throws IOException {
		LOGGER.debug("Copying " + length + " more bytes from the underlying stream");
		if (eof) return 0;
		int totallyRead = 0, read;
		do {
			read = underlying.read(buffer, offset + totallyRead, length - totallyRead);
			if (read < 0) {
				eof = true;
				break;
			}
			totallyRead += read;
		} while (length > totallyRead);
		readBytes += totallyRead;
		overflowOut.write(buffer, offset, totallyRead);
		overflowOut.flush();
		return totallyRead;
	}

	@Override
	public int read(byte[] b, int offset, int length) throws IOException {
		if (state != State.CONNECTED) throw new IllegalStateException("Cannot read from an unconnected stream");
		if (b != null) ByteArrays.ensureOffsetLength(b, offset, length);
		if (length == 0) return 0;
		int copied = 0; // The overall number of bytes copied onto b
		LOGGER.debug("Requested to read " + length);
		if (position < inspectable) {
			/* The first Math.min(inspectable-position,length) bytes should be taken from the buffer.
			 * inspectable - position = actual number of bytes available to be read */
			copied = Math.min(inspectable - (int)position, length);
			LOGGER.debug(" -> from memory buffer " + copied);
			System.arraycopy(buffer, (int)position, b, offset, copied);
			position += copied;
			offset += copied;
			length -= copied;
		}
		LOGGER.debug("After buffer, remaining to read " + length);
		/* If the underlying file is shorter than the buffer, we stop here.
		 * Notice that if we copied no byte, we must return 0 or -1 depending on
		 * whether we could have returned something (were length positive) or not.
		 */
		if (readBytes < buffer.length) {
			LOGGER.debug("Underlying is shorter than buffer; returning " + (copied > 0? copied : (position < readBytes? 0 : -1)));
			return copied > 0? copied : (position < readBytes? 0 : -1); // Underlying is shorter than buffer
		}
		/* If there is still some byte to be copied, we check whether some of
		 * them are available in the overflow file.
		 */
		if (length > 0) {
			// Some of them are already available in the overflow file: copy them
			int toBeReadFromOverflow = Math.min((int)(readBytes - position), length);
			int readFromOverflow = 0, c;
			do {
				c = overflowIn.read(b, offset + readFromOverflow, toBeReadFromOverflow - readFromOverflow);
				if (c < 0) break;
				readFromOverflow += c;
			} while (toBeReadFromOverflow > readFromOverflow);
			LOGGER.debug(" -> from overflow file " + toBeReadFromOverflow);
			copied += toBeReadFromOverflow;
			position += toBeReadFromOverflow;
			offset += toBeReadFromOverflow;
			length -= toBeReadFromOverflow;
		}
		LOGGER.debug("After file, remaining to read " + length);
		/* If there is still some byte to be copied, we copy it from the
		 * underlying input stream.
		 */
		if (length > 0) {
			// Should read from the underlying stream
			int c = copy(b, offset, length);
			LOGGER.debug(" -> copied from underlying stream " + c);
			copied += c;
			position += c;
		}
		LOGGER.debug("Returning " + (copied > 0? copied : -1));
		return copied > 0? copied : -1;
	}

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public long skip(final long n) throws IOException {
		long skipped = 0;
		for(int read; (read = read(b, 0, (int)Math.min(n - skipped, b.length))) > 0;) skipped += read;
		return skipped;
	}

	@Override
	public int read() throws IOException {
		if (state != State.CONNECTED) throw new IllegalStateException("Cannot read from an unconnected stream");
		if (position < buffer.length) // In memory
			return position < readBytes? buffer[(int)(position++)] & 0xFF : -1;
		else {
			if (position >= readBytes) { // Prefill buffer
				if (eof) return -1;
				int read = underlying.read();
				if (read < 0) {
					eof = true;
					return read;
				}
				readBytes++;
				overflowOut.write(read);
				overflowOut.flush();
			}
			position++;
			return overflowIn.read();
		}
	}

	/** Returns the current length of the overflow file.
	 *
	 * @return the length of the overflow file.
	 */
	public long overflowLength() {
		return overflowFile.length();
	}

	/** Reads the underlying input stream up to a given limit.
	 *
	 * @param limit the maximum number of bytes to be read, except for the memory buffer. More precisely, up to <code>Math.max(buffer.length,limit)</code>
	 *   bytes are read (because the buffer is filled at connection).
	 */
	public void fill(long limit) throws IOException {
		if (state != State.CONNECTED) throw new IllegalStateException("Cannot read from an unconnected stream");
		while (readBytes < limit &&  read(b, 0, (int)Math.min(limit - readBytes, b.length)) > 0);
	}

	/** Reads fully the underlying input stream.
	 * @see #fill(long)
	 */
	public void fill() throws IOException {
		fill(Integer.MAX_VALUE);
	}


	/** Reads fully the underlying input stream and rewinds.
	 *
	 * @see #fill()
	 * @see #rewind()
	 */
	public void fillAndRewind() throws IOException {
		fill();
		rewind();
	}

	/** Returns the overall length of this input stream.
	 * This method calls it with argument {@link Long#MAX_VALUE}.
	 *
	 *  @throws RuntimeException wrapping an {@link IOException} if the call to {@link #fill(long)} does.
	 */
	@Override
	public long length() {
		if (! lengthKnown) {
			final long position = position();
			try {
				fill(Long.MAX_VALUE); // Read to the end: necessary to know the length.
				lengthKnown = true;
				rewind();
				skip(position);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return readBytes;
	}

	@Override
	public long position() {
		return position;
	}
}
