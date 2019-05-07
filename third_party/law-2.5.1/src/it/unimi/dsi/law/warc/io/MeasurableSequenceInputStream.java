package it.unimi.dsi.law.warc.io;

import java.io.IOException;

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

import it.unimi.dsi.fastutil.io.MeasurableInputStream;

// RELEASE-STATUS: DIST

/**
 * A {@link it.unimi.dsi.fastutil.io.MeasurableInputStream} version of a {@link java.io.SequenceInputStream}.
 *
 */
public class MeasurableSequenceInputStream extends MeasurableInputStream {

	/** The array of streams. */
	private final MeasurableInputStream[] streams;

	/** The current stream ({@code null} when no input streams are left). */
	private MeasurableInputStream currentStream;

	/** The index in {@link #streams} of the {@link #currentStream}. */
	private int currentStreamIndex;

	/** The overall length of the sequence. */
	private final long length;

	/** The position (number of byetes read so far). */
	private long position;

	/**
	 * Constructs a sequence from an array of input streams.
	 *
	 * @param streams the streams (some of which may be {@code null}).
	 * @throws IOException
	 */
	public MeasurableSequenceInputStream(MeasurableInputStream... streams) throws IOException {
		if (streams == null) throw new NullPointerException();
		this.streams = streams;
		long l = 0;
		for (MeasurableInputStream is : streams) if (is != null) l += is.length();
		length = l;
		position = 0;
		currentStreamIndex = -1;
		nextStream();
	}

	/**
	 * Updates {@link #currentStream} and {@link #currentStreamIndex} to the next non-null
	 * input stream in {@link #streams}, closing the previous streams.
	 *
	 * @return the next stream.
	 */
	private boolean nextStream() {
		do currentStreamIndex++; while (currentStreamIndex < streams.length && streams[currentStreamIndex] == null);
		if (currentStreamIndex < streams.length) {
			currentStream = streams[currentStreamIndex];
			return true;
		} else return false;
	}

	public long length() {
		return length;
	}

	public long position() {
		return position;
	}

	public int read() throws IOException {
		if (currentStream == null) return -1;
		int b;
		do {
			b = currentStream.read();
			if (b != -1) position += 1;
		} while (b == -1 && nextStream());
		return b;
	}

	@Override
	public int read(byte[] buf, int offset, int length) throws IOException {
		if (buf == null) throw new NullPointerException();
		if ((offset < 0) || (offset > buf.length) || (length < 0) ||
				   ((offset + length) > buf.length) || ((offset + length) <  0))
			    throw new IndexOutOfBoundsException();
		if (currentStream  == null) return -1;
		if (length == 0) return 0;
		int r;
		do {
			r = currentStream.read(buf, offset, length);
			if (r != -1) position += r;
		} while (r == -1 && nextStream());
		return r;
	}

	@Override
	public void close() throws IOException {
		for (MeasurableInputStream i : streams) if (i != null) i.close();
	}

}
