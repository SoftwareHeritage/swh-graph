package it.unimi.dsi.law.warc.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

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
 * A class that decorates an {@link java.io.InputStream} to obtain a
 * {@link it.unimi.dsi.fastutil.io.MeasurableInputStream}.
 *
 * <p> This class serves two purpose: wrap an input stream so that no more than a certain number of
 * bytes may be read from it (and that the total number of bytes read so far is available). Moreover,
 * if a {@link java.util.zip.CRC32} is given, the content is also checked using it.
 *
 * <p> Observe that the underlying stream can be {@code null} if the empty constructor is
 * called but {@link #setInput(InputStream, long, CRC32)} is not called; this will lead to
 * {@link java.lang.NullPointerException} in almost every call.
 */

public class BoundedCountingInputStream extends MeasurableInputStream {

	/** The underlying input stream. */
	private InputStream is;

	/** The (cached) length of this stream (the minimum among this stream bound and the length of the underlying stream;
	 *  if the latter is not measurable, it is considered to have infinite length).
	 */
	private long length;

	/** The bound. */
	private long bound;

	/** The current position. */
	private long position;

	/** Tells if the underlying stream has reached its end. */
	private boolean eofReached;

	/** A class to compute the crc of read bytes. */
	public CRC32 crc;

	/**
	 * Builds the bounded stream.
	 *
	 * <p> Before actually using an object constructed with this,
	 * {@link #setInput(InputStream, long, CRC32)} must be called.
	 */
	public BoundedCountingInputStream() {}

	/**
	 * Builds the bounded stream.
	 *
	 * @param is the stream.
	 * @param bound the maximum number of bytes that can be read.
	 * @param crc if not {@code null}, it will be used to compute the crc of read bytes.
	 * @throws IOException
	 */
	public BoundedCountingInputStream(final InputStream is, final long bound, final CRC32 crc) throws IOException {
		setInput(is, bound, crc);
	}

	/**
	 * Builds the bounded stream.
	 *
	 * @param is the stream.
	 * @param bound the maximum number of bytes that can be read.
	 * @throws IOException
	 */
	public BoundedCountingInputStream(final InputStream is, final long bound) throws IOException {
		setInput(is, bound);
	}

	/**
	 * Resets the bounded stream fields, for reusing it.
	 *
	 * @param is the stream.
	 * @param bound the maximum number of bytes that can be read.
	 * @param crc if not {@code null}, it will be used to compute the crc of read bytes.
	 * @throws IOException
	 */
	public void setInput(final InputStream is, final long bound, final CRC32 crc) throws IOException {
		if (is == null) throw new IllegalArgumentException();
		this.is = is;
		this.bound = bound;
		this.crc = crc;
		this.position = 0;
		eofReached = false;
		long isLength = Long.MAX_VALUE;
		if (is instanceof MeasurableInputStream) try {
			isLength = ((MeasurableInputStream)is).length();
		} catch (UnsupportedOperationException e) {}
		length = Math.min(isLength, bound);
	}

	/**
	 * Resets the bounded stream fields, for reusing it.
	 *
	 * @param is the stream.
	 * @param bound the maximum number of bytes that can be read.
	 * @throws IOException
	 */
	public void setInput(final InputStream is, final long bound) throws IOException {
		setInput(is, bound, null);
	}

	@Override
	public int available() throws IOException {
		return (int)Math.min(is.available(), bound - position);
	}

	public int read() throws IOException {
		if (position >= bound || eofReached) return -1;
		int b = is.read();
		if (b == -1)
			eofReached = true;
		else {
			position++;
			if (crc != null) crc.update(b);
		}
		return b;
	}

	@Override
	public int read(final byte[] buf, final int offset, final int length) throws IOException {
		if (length == 0) return 0;
		if (position >= bound || eofReached) return -1;
		int read = is.read(buf, offset, (int)Math.min(length, bound - position));
		if (read == -1)
			eofReached = true;
		else {
			position += read;
			if (crc != null) crc.update(buf, offset, read);
		}
		return read;
	}

	public long length() throws IOException {
		return length;
	}

	public long position() {
		return position;
	}

}
