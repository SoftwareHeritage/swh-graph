package it.unimi.dsi.law.warc.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.lang.ArrayUtils;
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

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.law.bubing.util.BURL;
import it.unimi.dsi.law.warc.util.HttpResponse;
import it.unimi.dsi.law.warc.util.Util;


// RELEASE-STATUS: DIST

/**
 * A class to read/write WARC/0.9 records in compressed form (for format details, please see the <a
 * href='http://archive-access.sourceforge.net/warc/warc_file_format-0.9.html'>WARC</a> and <a
 * href='http://www.gzip.org/zlib/rfc1952.pdf'>GZip</a> format specifications).
 *
 * <p> Records written/read with this class use the <code>skip-lengths</code> as detailed in
 * section 10.2 of warc specification.
 *
 * <p> Moreover the <code>NAME</code> optional gzip header field contains the
 * {@link it.unimi.dsi.law.warc.io.WarcRecord.Header#recordId} and the <code>COMMENT</code>
 * optional gzip header field contains the value of the <code>anvl-filed</code> corresponding to
 * the {@link it.unimi.dsi.law.warc.util.HttpResponse#DIGEST_HEADER} key, if present, or the
 * {@link it.unimi.dsi.law.warc.io.WarcRecord.Header#recordId}, followed by tab and then by the
 * {@link it.unimi.dsi.law.warc.io.WarcRecord.Header#subjectUri}.
 *
 * <p> As for a {@link it.unimi.dsi.law.warc.io.WarcRecord}, to write a record, set {@link #header}
 * and {@link #block} appropriately and then call {@link #write(OutputStream)}. After such call,
 * some {@link #header} fields will be modified and the {@link #gzheader} fields will be set to
 * reflect the write operation.
 *
 * <p> Again, as in the case of a {@link it.unimi.dsi.law.warc.io.WarcRecord}, to perform a sequence
 * of consecutive read/skip, call {@link #read(FastBufferedInputStream)} or
 * {@link #skip(FastBufferedInputStream)}. After a read, the {@link #block} can (but it is not
 * required to) be read to obtain the read data. The {@link WarcRecord.Header#contentType} field
 * can be used to determine how to parse the content of {@link #block}.
 *
 * <p> As an implementation note: skipping just populates the {@link #gzheader} fields and returns
 * the value of the <code>compressed-skip-length</code> field of the skipped record. On the other
 * hand, reading parses the gzip header as well as the warc <code>header</code> and sets all the
 * {@link #header} and {@link #gzheader} fields appropriately, and hence sets {@link #block} so that
 * it refers to the <code>block</code> part of the record. After a full read (a read that leaves
 * less than {@link #PARTIAL_UNCOMPRESSED_READ_THRESHOLD} bytes in {@link #uncompressedRecordStream})
 * the CRC found in the gzip file is checked against the CRC computed over the record; partial reads
 * can check the CRC calling {@link #checkCRC(FastBufferedInputStream)} that will consume
 * {@link #uncompressedRecordStream} to compute the CRC.
 *
 * <p>This object can be reused for non-consecutive writes on different streams. On the other hand,
 * to reuse this object for non-consecutive read/skip, the method {@link #resetRead()} must be
 * called any time a read/skip does not follow a read/skip from the same stream.
 *
 * <p>This class uses internal buffering, hence it is not thread safe.
 */
@SuppressWarnings("javadoc")
public class GZWarcRecord extends WarcRecord {
	private final static Logger LOGGER = LoggerFactory.getLogger(GZWarcRecord.class);

	@SuppressWarnings("hiding")
	final public static boolean ASSERTS = true;

	/** Tells what method to use to skip bytes in the input stream. It's here for profiling purposes. */
	public static final boolean USE_POSITION_INSTEAD_OF_SKIP = false;

	/** A class to contain fields contained in the gzip header. */
	public static class GZHeader {

		/** The <code>compressed-skip-length</code> warc-required extra gzip field. */
		public int compressedSkipLength;

		/** The <code>uncompressed-skip-length</code> warc-required extra gzip field. */
		public int uncompressedSkipLength;

		/** The <code>mtime</code> gzip field. */
		public int mtime;

		/** The (optional) <code>name</code> gzip field. Here is used to contain {@link WarcRecord.Header#recordId}) */
		public byte[] name;

		/** The (optional) <code>comment</code> gzip field. Here is used to contain {@link WarcRecord.Header#subjectUri}) */
		public byte[] comment;

		@Override
		public String toString() {
			MutableString s = new MutableString();
			s.append("compressedSkipLength: ");
			s.append(compressedSkipLength);
			s.append(", uncompressedSkipLength: ");
			s.append(uncompressedSkipLength);
			s.append(", mtime: ");
			s.append(mtime);
			s.append(", name: ");
			s.append(name == null ? "<null>" : Util.getString(name));
			s.append(", comment: ");
			s.append(comment == null ? "<null>" : Util.getString(comment));
			return s.toString();
		}

		@Override
		public int hashCode() {
			// TODO can we do better?
			return Util.getString(name).hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (! (o instanceof GZWarcRecord.GZHeader)) return false;
			GZWarcRecord.GZHeader h = (GZWarcRecord.GZHeader)o;
			if (compressedSkipLength != h.compressedSkipLength) return false;
			if (uncompressedSkipLength != h.uncompressedSkipLength) return false;
			if (mtime != h.mtime) return false;
			if (! Arrays.equals(name, h.name)) return false;
			if (! Arrays.equals(comment, h.comment)) return false;
			return true;
		}

	}

	/* GZip constants TODO: comment! */

	private static final byte XFL = Deflater.BEST_COMPRESSION;

	@SuppressWarnings("unused")
	private static final byte FTEXT = 1 << 0, FHCRC = 1 << 1, FEXTRA = 1 << 2, FNAME = 1 << 3, FCOMMENT = 1 << 4;

	private static final byte[]	GZIP_START = new byte[] {
									(byte)0x1F, (byte)0x8B,		// ID1 ID2
									Deflater.DEFLATED, 			// CM
									FEXTRA | FNAME | FCOMMENT },	// FLG
								XFL_OS = new byte[] { XFL, (byte)0xFF }, // unknown os
								SKIP_LEN = new byte[] { (byte)'s', (byte)'l' };

	private static final short SUB_LEN = 8; //  2 ints (compressedSkipLength, uncompressedSkipLength)

	private static final short XLEN = 4 + SUB_LEN; // 2 byte (SI1 + SI2) + 1 short (SUB_LEN)

	private static final short TRAILER_LEN = 8; // 2 ints (CRC32, ISIZE)

	private static final int FIX_LEN =	GZIP_START.length +
										4 + // 1 int (MTIME)
										XFL_OS.length +
										(2 + XLEN) + // 1 short (XLEN) + EXTRA bytes
										TRAILER_LEN;

	/** If {@link #uncompressedRecordStream} contains more than this amount of bytes, the last read is considered partial. */
	private static final int PARTIAL_UNCOMPRESSED_READ_THRESHOLD = 16; // must be >= 4, the two last CRLF present in any record

	/** The buffer size used by the {@link #uncompressedRecordStream}. */
	private static final int UNCOMPRESSED_RECORD_STREAM_BUFFER_SIZE = 1024;

	/** The deflater used to compress the record. */
	private final Deflater deflater = new Deflater(XFL, true);

	/** The inflater used to decompress the record. */
	private final Inflater inflater = new Inflater(true);

	/** An output stream used by {@link #write(OutputStream)} to cache the compressed record. */
	private final FastByteArrayOutputStream compressedOutputStream = new FastByteArrayOutputStream();

	/** The size of {@link #headerBuffer}; must be enough to contain NAME, or COMMENT, or GZIP_START. */
	private static final int HEADER_BUFFER_SIZE = 16384;

	/** A buffer used in reading/writing the GZip header. */
	private final byte[] headerBuffer = new byte[HEADER_BUFFER_SIZE];

	/** The position of the first byte of the last read GZip header. It is -1 when no header has been read, or when the trailed of the last header was already read. */
	private long positionOfLastGZHeader;

	/** The <code>compressed-skip-length</code> found in the last read GZip header (set by {@link #readGZHeader(FastBufferedInputStream)}). */
	private long compressedDataLengthInLastGZHeader;

	/** An input stream passed to {@link WarcRecord#read(FastBufferedInputStream)} as the last found uncompressed block ({@code null} in case of a skip). */
	private FastBufferedInputStream uncompressedRecordStream;

	/** The GZip headers used by this object. */
	final public GZHeader gzheader = new GZHeader();

	public GZWarcRecord() {
		crc = new CRC32();
		positionOfLastGZHeader = -1;
	}

	@Override
	public void resetRead() {
		positionOfLastGZHeader = -1;
	}

	@Override
	public long skip(FastBufferedInputStream in) throws IOException, FormatException {
		if (readGZHeader(in) == -1) return -1;
		uncompressedRecordStream = null; // so that the readGZTrailer will not consume it and compute the CRC
		readGZTrailer(in);
		return gzheader.compressedSkipLength;
	}

	@Override
	public long read(FastBufferedInputStream in) throws IOException, FormatException {

		if (readGZHeader(in) == -1) return -1;

		// compressed blocks

		inflater.reset();
		crc.reset();
		final long reminingCompressedBytes = gzheader.compressedSkipLength - (in.position() - positionOfLastGZHeader) - TRAILER_LEN;
		if (ASSERTS) assert reminingCompressedBytes > 0; // we always have at least the <code>header-line</code> and the two last CRLFs
		InflaterInputStream gzin = new InflaterInputStream(new BoundedCountingInputStream(in, reminingCompressedBytes), inflater);
		uncompressedRecordStream = new FastBufferedInputStream(new BoundedCountingInputStream(gzin, gzheader.uncompressedSkipLength, crc), UNCOMPRESSED_RECORD_STREAM_BUFFER_SIZE);
		super.resetRead(); 	// reading from gzip does not require to consume previous input
		super.read(uncompressedRecordStream);

		return compressedDataLengthInLastGZHeader;
	}

	@Override
	public void write(OutputStream out) throws IOException {

		byte[] buffer = headerBuffer; // for efficiency

		/* prepare the compressed block and uncompressed crc */

		deflater.reset();
		compressedOutputStream.reset();
		DeflaterOutputStream gzout = new DeflaterOutputStream(compressedOutputStream, deflater);
		crc.reset();
		super.write(gzout);
		gzout.finish();
		gzout = null;

		/* fill gzheader */

		final byte[] recordByteRepresentation = Util.getASCIIBytes(header.recordId.toString());
		final String digest = header.anvlFields.get(HttpResponse.DIGEST_HEADER);

		gzheader.name = recordByteRepresentation;

		int commentLength = 0;
		if (digest != null) {
			final byte[] digestByteRepresentation = Util.getASCIIBytes(digest);
			commentLength = digestByteRepresentation.length;
			System.arraycopy(digestByteRepresentation, 0, buffer, 0, commentLength);
		} else {
			commentLength = recordByteRepresentation.length;
			System.arraycopy(recordByteRepresentation, 0, buffer, 0, commentLength);
		}
		buffer[commentLength++] = '\t';
		final byte[] subjectUriByteRepresentation = BURL.toByteArray(header.subjectUri);
		System.arraycopy(subjectUriByteRepresentation, 0, buffer, commentLength, subjectUriByteRepresentation.length);
		commentLength += subjectUriByteRepresentation.length;
		gzheader.comment = ArrayUtils.subarray(buffer, 0, commentLength);

		gzheader.compressedSkipLength = FIX_LEN + (gzheader.name.length + 1) + (gzheader.comment.length + 1) + compressedOutputStream.length;
		gzheader.uncompressedSkipLength = (int)(header.dataLength & 0xFFFFFFFF);
		gzheader.mtime = (int)(header.creationDate.getTime() / 1000);

		/* write */

		// ID1 ID2 CM FLG

		out.write(GZIP_START);

		// MTIME

		writeLEInt(out, gzheader.mtime);

		// XFL OS

		out.write(XFL_OS);

		/* EXTRA begin */

		// XLEN

		writeLEShort(out, XLEN);

		// SI1 SI2 (as in warc spec)

		out.write(SKIP_LEN);

		// LEN

		writeLEShort(out, SUB_LEN);

		// compressed-skip-length (as in warc spec)

		writeLEInt(out, gzheader.compressedSkipLength);

		// uncompressed length (as in warc spec)

		writeLEInt(out, gzheader.uncompressedSkipLength);

		/* EXTRA end */

		// NAME

		out.write(gzheader.name);
		out.write(0);

		// COMMENT

		out.write(gzheader.comment);
		out.write(0);

		// compressed blocks

		out.write(compressedOutputStream.array, 0, compressedOutputStream.length);

		// CRC32

		writeLEInt(out, (int)(crc.getValue() & 0xFFFFFFFF));

		// ISIZE

		writeLEInt(out, gzheader.uncompressedSkipLength);

	}

	public void checkCRC(FastBufferedInputStream in) throws IOException, FormatException {
		if (positionOfLastGZHeader == -1 || uncompressedRecordStream == null) throw new IllegalStateException();
		consumeUncompressedRecord();
		readGZTrailer(in);
	}

	@Override
	public String toString() {
		return gzheader.toString() + "\n" + header.toString();
	}

	private long readGZHeader(FastBufferedInputStream in) throws IOException, FormatException {

		byte[] buffer = headerBuffer; // local copy for efficiency reasons

		// if we haven't done it yet, consume the trailer of the previous read

		if (positionOfLastGZHeader != -1) readGZTrailer(in);

		// ID1 ID2 CM FLG

		positionOfLastGZHeader = in.position();
		if (in.read(buffer, 0, 4) == -1) return -1;

		if (buffer[0] != GZIP_START[0] || buffer[1] != GZIP_START[1]) throw new FormatException("Missing GZip magic numbers, found: " + buffer[0] + " " + buffer[1]);
		if (buffer[2] != Deflater.DEFLATED) throw new FormatException("Unknown compression method: " + buffer[2]);
		int flg = buffer[3];

		// MTIME

		gzheader.mtime = readLEInt(in);

		// XFL OS (ignored)

		in.read(buffer, 0, 2);

		/* EXTRA begin */

		gzheader.compressedSkipLength = -1;

		if ((flg & FEXTRA) != 0) {

			// XLEN

			short xlen = readLEShort(in);

			while (xlen > 0) {

				// SI1 SI2

				in.read(buffer, 0, 2);

				// LEN

				short len = readLEShort(in);

				if (buffer[0] == SKIP_LEN[0] && buffer[1] == SKIP_LEN[1]) {
					compressedDataLengthInLastGZHeader = gzheader.compressedSkipLength = readLEInt(in);
					gzheader.uncompressedSkipLength = readLEInt(in);
				} else in.read(buffer, 0, len);

				xlen -= len + 4; // 2 bytes (SI1,  SI2) + 1 short (LEN)

			}
		}

		if (gzheader.compressedSkipLength < 0) throw new FormatException("Missing SL extra field, or negative compressed-skip-length");

		/* EXTRA end */

		// NAME

		if ((flg & FNAME) != 0) {
			int l = 0, b;
			while ((b = in.read()) != 0) {
				 buffer[l++] = (byte)b;
			}
			gzheader.name = ArrayUtils.subarray(buffer, 0, l);
		}

		// COMMENT

		if ((flg & FCOMMENT) != 0) {
			int l = 0, b;
			while ((b = in.read()) != 0) {
				 buffer[l++] = (byte)b;
			}
			gzheader.comment = ArrayUtils.subarray(buffer, 0, l);
		}

		// HCRC

		if ((flg & FHCRC) != 0) {
			in.read(buffer, 0, 2);
		}

		return compressedDataLengthInLastGZHeader;
	}

	private void readGZTrailer(FastBufferedInputStream in) throws IOException, FormatException {

		if (positionOfLastGZHeader == -1) return; // we haven't read any new header

		// possibly position correctly according to previous read

		boolean consumed = false;
		if (uncompressedRecordStream != null) {
			final long remaining = uncompressedRecordStream.length() - uncompressedRecordStream.position();
			if (ASSERTS) assert remaining >= 0;
			if (0 < remaining && remaining < PARTIAL_UNCOMPRESSED_READ_THRESHOLD) { // > 0 is needed to avoid unnecessary consume
				consumeUncompressedRecord(); // we update the CRC for complete reads
				consumed = true;
			} else LOGGER.debug("Omitting CRC check, since the last read was partial.");
		} else LOGGER.debug("Omitting CRC check, since coming from a skip.");

		long newPosition = positionOfLastGZHeader + compressedDataLengthInLastGZHeader - TRAILER_LEN;
		if (ASSERTS) assert newPosition >= in.position();
		if (USE_POSITION_INSTEAD_OF_SKIP)
			in.position(newPosition);
		else
			in.skip(newPosition - in.position());

		// CRC32

		final int expectedCrc = readLEInt(in);
		if (consumed) {
			final int actualCrc = (int)(crc.getValue() & 0xFFFFFFFF);
			if (expectedCrc != actualCrc) throw new FormatException("CRC32 mismatch, expected: " + expectedCrc + ", actual: " + actualCrc);
			else LOGGER.debug("CRC check OK.");
		}

		// ISIZE

		final int iSize = readLEInt(in);
		if (gzheader.uncompressedSkipLength != iSize) throw new FormatException("Length mismatch between (warc) extra gzip fields uncompressed-skip-length (" + gzheader.uncompressedSkipLength + ") and ISIZE (" + iSize + ")");

		//	we have consumed the trailer

		positionOfLastGZHeader = -1;

	}

	private void consumeUncompressedRecord() throws IOException {
		if (ASSERTS) assert uncompressedRecordStream.length() - uncompressedRecordStream.position() >= 4;
		byte[] b = new byte[1024];
		while (uncompressedRecordStream.read(b) != -1); // we use read instead of skip because we want the CRC to be updated!
		uncompressedRecordStream.skip(Long.MAX_VALUE);
	}

	private static int readLEInt(InputStream in) throws IOException {
		int i = in.read() & 0xFF;
		i |= (in.read() & 0xFF) << 8;
		i |= (in.read() & 0xFF) << 16;
		i |= (in.read() & 0xFF) << 24;
		return i;
	}

	private static short readLEShort(InputStream in) throws IOException {
		short s = (byte)in.read();
		s |= (byte)in.read() << 8;
		return s;
	}

	private static void writeLEInt(OutputStream out, int i) throws IOException {
		out.write((byte)i);
		out.write((byte)((i >> 8) & 0xFF));
		out.write((byte)((i >> 16) & 0xFF));
		out.write((byte)((i >> 24) & 0xFF));
	}

	private static void writeLEShort(OutputStream out, short s) throws IOException {
		out.write((byte)s);
		out.write((byte)((s >> 8) & 0xFF));
	}

}

