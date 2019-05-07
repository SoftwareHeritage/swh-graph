
package it.unimi.dsi.law.warc.io;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
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

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream.LineTerminator;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.law.bubing.util.BURL;
import it.unimi.dsi.law.warc.util.Util;

// RELEASE-STATUS: DIST

/**
 * A class to read/write WARC/0.9 records (for format details, please see the <a
 * href='http://archive-access.sourceforge.net/warc/warc_file_format-0.9.html'>WARC</a> format specifications).
 *
 * <p> To write a record, set {@link #header} and {@link #block} appropriately and then call
 * {@link #write(OutputStream)}. After such call, the {@link WarcRecord.Header#dataLength} field
 * of the {@link #header} will be modified to reflect the write operation.
 *
 * <p> To perform a sequence of consecutive read/skip, call {@link #read(FastBufferedInputStream)}
 * or {@link #skip(FastBufferedInputStream)}. After a read, the {@link #block} can (but it is not
 * required to) be read to obtain the read data. The {@link WarcRecord.Header#contentType contentType}
 * field of the {@link #header} can be used to determine how to further process the content of
 * {@link #block}.
 *
 * <p> As an implementation note: skipping just returns the value of the <code>data-length</code>
 * field of the skipped record. On the other hand, reading parses the <code>header</code> and sets
 * all the {@link #header} fields appropriately; hence it sets {@link #block} so that it refers to
 * the <code>block</code> part of the record. Observe that since {@link #block} is just a "view"
 * over the underlying stream, its content, or position, are not guaranteed to remain the same after
 * a consecutive read/skip on the same stream.
 *
 * <p>This object can be reused for non-consecutive writes on different streams. On the other hand,
 * to reuse this object for non-consecutive read/skip, the method {@link #resetRead()} must be
 * called any time a read/skip does not follow a read/skip from the same stream.
 *
 * <p> This class uses internal buffering, hence it is not thread safe.
 */
public class WarcRecord {
//	private final static Logger LOGGER = LoggerFactory.getLogger(WarcRecord.class);
	public static final boolean DEBUG = false;
	public static final boolean ASSERTS = true;

	/** Tells what method to use to skip bytes in the input stream. It's here for profiling purposes. */
	public static final boolean USE_POSITION_INSTEAD_OF_SKIP = false;

	/** Content types. */
	public static enum ContentType {
		HTTP("message/http"),
		HTTPS("message/https");
		public final byte[] byteRepresentation;
		ContentType(String name) {
			byteRepresentation = Util.getASCIIBytes(name);
		}
	};
	public static final Object2ObjectMap<byte[],ContentType> BYTE_REPRESENTATION_TO_CONTENT_TYPE =
		new Object2ObjectOpenCustomHashMap<byte[],ContentType>(ByteArrays.HASH_STRATEGY);

	/** Record types. */
	public static enum RecordType {
		WARCINFO("warcinfo"),
		RESPONSE("response"),
		RESOURCE("resource"),
		REQUEST("request"),
		METADATA("metadata"),
		REVISIT("revisit"),
		CONVERSION("conversion"),
		CONTINUATION("continuation");
		public final byte[] byteRepresentation;
		RecordType(String name) {
			byteRepresentation = Util.getASCIIBytes(name);
		}
	};
	public static final Object2ObjectMap<byte[],RecordType> BYTE_REPRESENTATION_TO_RECORD_TYPE =
		new Object2ObjectOpenCustomHashMap<byte[],RecordType>(ByteArrays.HASH_STRATEGY);

	static {
		for (ContentType ct : ContentType.values())
			BYTE_REPRESENTATION_TO_CONTENT_TYPE.put(ct.byteRepresentation, ct);
		for (RecordType rt : RecordType.values())
			BYTE_REPRESENTATION_TO_RECORD_TYPE.put(rt.byteRepresentation, rt);
	}

	/** A class to contain fields contained in the warc <code>header</code>. */
	public static class Header {

		/* These are all public to avoid getter/setters. */

		/** The warc <code>data-length</code>. */
		public long dataLength;

		/** The warc <code>record-type</code>. */
		public RecordType recordType;

		/** The warc <code>subject-uri</code>. */
		public URI subjectUri;

		/** The warc <code>creation-date</code>. */
		public Date creationDate;

		/** The warc <code>content-type</code>. */
		public ContentType contentType;

		/** The warc <code>record id</code>. */
		public UUID recordId;

		/** The warc <code>anvl-field</code>s. */
		public final Map<String,String> anvlFields = new Object2ObjectOpenCustomHashMap<String,String>(Util.CASE_INSENSITIVE_STRING_HASH_STRATEGY);

		/**
		 * Copies this heaer fields from another header.
		 *
		 * @param header the header to copy from.
		 */
		public void copy(final Header header) {
			dataLength = header.dataLength;
			recordType = header.recordType;
			subjectUri = header.subjectUri;
			creationDate = header.creationDate;
			contentType = header.contentType;
			recordId = header.recordId;
			anvlFields.clear();
			anvlFields.putAll(header.anvlFields);
		}

		@Override
		public int hashCode() {
			return recordId.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (! (o instanceof WarcRecord.Header)) return false;
			if (! recordId.equals(((WarcRecord.Header)o).recordId)) return false;
			return true;
		}

		@Override
		public String toString() {
			MutableString s = new MutableString();
			s.append("dataLength: ");
			s.append(dataLength);
			s.append(", recordType: ");
			s.append(recordType);
			s.append(", subjectUri: ");
			s.append(subjectUri);
			s.append(", creationDate: ");
			s.append(creationDate);
			s.append(", contentType: ");
			s.append(contentType);
			s.append(", recordId: ");
			s.append(recordId);
			s.append(", anvlFields: ");
			s.append(anvlFields);
			return s.toString();
		}

	}

	/** An exception to denote parsing errors during reads. */
	public static class FormatException extends Exception {
		private static final long serialVersionUID = -1L;
		public FormatException(String s) {
			super(s);
		}
	}

	/** A minimalistic parser used for header parsing. */
	private static class MinimalisticParser {
		@SuppressWarnings("hiding")
		private static final boolean DEBUG = false;
		private int start, end, length;
		private byte[] buf;
		@SuppressWarnings("unused")
		public void setInput(byte[] buf) {
			setInput(buf, 0, buf.length);
		}
		public void setInput(byte[] buf, int offset, int length) {
			this.buf = buf;
			start = offset;
			end = start;
			this.length = length;
		}
		public void positionAtNextWord() {
			start = end;
			while ((start < length) && Character.isWhitespace(buf[start])) start++;
			end = start;
			while ((end < length) && ! Character.isWhitespace(buf[end])) end++;
			if (DEBUG) System.err.println("Next word '" + Util.getString(buf, start, end - start) + "'");
		}
		public boolean startsWith(byte[] m) {
			int i = 0, ml = m.length;
			while (i < ml && start + i < length && buf[start + i] == m[i]) i++;
			return i == ml;
		}
		public int asInt() {
			int i = start, val = 0;
			while (i < end) {
				val = val * 10 + (buf[i] - (byte)'0');
				i++;
			}
			if (DEBUG) System.err.println("Returned long " + val);
			return val;
		}
		public String asAsciiSting() {
			String ret = Util.getString(buf, start, end - start);
			if (DEBUG) System.err.println("Returned string '" + ret + "'");
			return ret;
		}
		public byte[] asByteArray() {
			byte[] b = new byte[end - start];
			System.arraycopy(buf, start, b, 0, end - start);
			return b;
		}
	}

	/** The default size of the internal buffer used for headers read/write. */
	public static final int DEFAULT_BUFFER_SIZE = 4 * 1024;

	/** The {@link java.nio.charset.Charset} used to encode <code>anvl-field</code>s. */
	private static final Charset ANVL_CHARSET = Charset.forName("UTF-8");

	/** Some constant strings in their byte equivalent. */
	public static final byte[]		WARC_ID = Util.getASCIIBytes("warc/0.9"),
									UUID_FIELD_NAME = Util.getASCIIBytes("uuid"),
									CRLF = new byte[] { 0x0D, 0x0A };

	/** The terminator used reading the warc header with {@link FastBufferedInputStream#readLine(byte[], java.util.EnumSet)}. */
	final EnumSet<LineTerminator> LINE_TERMINATOR = EnumSet.of(FastBufferedInputStream.LineTerminator.CR_LF);

	/** A formatter for the <code>creation-date</code> field of warc <code>header-line</code>. */
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

	/** The internal buffer used for headers read/write. */
	private final byte[] buffer;

	/** The instance of header parser. */
	private final MinimalisticParser minimalisticParser = new MinimalisticParser();

	/** The position of the first byte of the last read <code>header</code> (set by {@link #readHeaderLine(FastBufferedInputStream)}). */
	private long positionOfLastHeader;

	/** The <code>data-length</code> found in the last read <code>header</code> (set by {@link #readHeaderLine(FastBufferedInputStream)}). */
	private long dataLengthInLastHeader;

	/** The class used in {@link #write(OutputStream)} to compute CRC32 of the content for {@link GZWarcRecord}. If {@code null} the crc will not be updated. */
	protected CRC32 crc = null;

	/** The warc <code>header</code>. */
	public final Header header = new Header();

	/** The warc <code>block</code>. */
	public MeasurableInputStream block;

	/** Builds a warc record.
	 *
	 * @param buffer the buffer used for header read/write buffering.
	 */
	public WarcRecord(byte[] buffer) {
		this.buffer = buffer;
		positionOfLastHeader = -1;
	}

	/**
	 * Builds a warc record.
	 *
	 * It will allocate an internal buffer of size {@link #DEFAULT_BUFFER_SIZE} bytes to buffer
	 * header read/writes.
	 */
	public WarcRecord() {
		this(new byte[DEFAULT_BUFFER_SIZE]);
	}

	/**
	 * Copies this warc record fields from another warc record.
	 *
	 * @param record the record to copy from.
	 */
	public void copy(final WarcRecord record) {
		header.copy(record.header);
		block = record.block;
	}

	/**
	 * A method to allow the reuse of the present object for non consecutive reads.
	 */
	public void resetRead() {
		positionOfLastHeader = -1;
	}

	/**
	 * A method to skip a record from an {@link java.io.InputStream}.
	 *
	 * @param bin the {@link FastBufferedInputStream} to read from.
	 * @return the value of the <code>data-length</code>, or -1 if eof has been reached.
	 */
	public long skip(FastBufferedInputStream bin) throws IOException, FormatException {
		if (readHeaderLine(bin) == -1) return -1;
		final long newPosition = positionOfLastHeader + dataLengthInLastHeader;
		if (ASSERTS) assert newPosition >= bin.position();
		if (USE_POSITION_INSTEAD_OF_SKIP)
			bin.position(newPosition);
		else
			bin.skip(newPosition - bin.position());
		return dataLengthInLastHeader;
	}

	/**
	 * A method to read a record from an {@link java.io.InputStream}.
	 *
	 * @param bin the {@link FastBufferedInputStream} to read from.
	 * @return the value of the <code>data-length</code>, or -1 if eof has been reached.
	 */
	public long read(FastBufferedInputStream bin) throws IOException, FormatException {

		// read the header-line

		if (readHeaderLine(bin) == -1) return -1;
		header.dataLength = dataLengthInLastHeader;

		// parse the rest of it

		minimalisticParser.positionAtNextWord();
		header.recordType = BYTE_REPRESENTATION_TO_RECORD_TYPE.get(minimalisticParser.asByteArray());

		minimalisticParser.positionAtNextWord();
		header.subjectUri = BURL.parse(minimalisticParser.asAsciiSting());

		minimalisticParser.positionAtNextWord();
		try {
			header.creationDate = DATE_FORMAT.parse(minimalisticParser.asAsciiSting());
		}
		catch (ParseException e) {
			throw new FormatException("Error parsing creation-date: " + e.getMessage());
		}

		minimalisticParser.positionAtNextWord();
		header.contentType = BYTE_REPRESENTATION_TO_CONTENT_TYPE.get(minimalisticParser.asByteArray());

		minimalisticParser.positionAtNextWord();
		String recordIdAsString = minimalisticParser.asAsciiSting();
		if (! minimalisticParser.startsWith(UUID_FIELD_NAME)) throw new FormatException("Unknown type of record-id." + recordIdAsString);
		try {
			header.recordId = UUID.fromString(recordIdAsString.substring(UUID_FIELD_NAME.length + 1));
		}
		catch (IllegalArgumentException e) {
			throw new FormatException("Error parsing record-id '" + recordIdAsString + "'; " + e.getMessage());
		}

		// done with the header, now anvl-fields and block
		header.anvlFields.clear();
		Util.readANVLHeaders(bin, header.anvlFields, ANVL_CHARSET);
		final long blockLength = header.dataLength - (bin.position() - positionOfLastHeader) - 4; // the two ending CRLF
		if (ASSERTS) assert blockLength >= 0;
		block = new BoundedCountingInputStream(bin, blockLength);

		return dataLengthInLastHeader;
	}

	/**
	 * A method to write this record to an {@link java.io.OutputStream}.
	 *
	 * @param out where to write.
	 */
	public void write(OutputStream out) throws IOException {

		// prebuffer the header

		FastByteArrayOutputStream hbuf = new FastByteArrayOutputStream(buffer);
		header.dataLength = prebufferHeader(hbuf);

		// header-line start (warc-id and size)

		byte[] dataLengthAsBytes = Util.getASCIIBytes(Long.toString(header.dataLength));

		out.write(WARC_ID);
		out.write(' ');
		out.write(dataLengthAsBytes);
		out.write(' ');

		// prebuffered stuff (already CRLF terminated)

		out.write(hbuf.array, 0, hbuf.length);

		// update crc

		if (crc != null) {
			crc.update(WARC_ID);
			crc.update(' ');
			crc.update(dataLengthAsBytes);
			crc.update(' ');
			crc.update(hbuf.array, 0, hbuf.length);
		}
		hbuf = null;

		// block

		int read;
		long remaining = block.length();

		do {
			read = block.read(buffer, 0, (int)Math.min(remaining, buffer.length));
			if (read == -1) break;
			out.write(buffer, 0, read);
			if (crc != null) crc.update(buffer, 0, read);
			remaining -= read;
		} while (remaining > 0);

		assert remaining == 0 : remaining;

		// two ending CRLFs

		out.write(CRLF);
		out.write(CRLF);

		if (crc != null) {
			crc.update(CRLF);
			crc.update(CRLF);
		}

	}

	@Override
	public String toString() {
		return header.toString();
	}

	/**
	 * Prebuffers the warc <code>header</code>.
	 *
	 * <p> This method writes in the given stream the header part of the warc record starting from
	 * <code>record-type</code> filed included to the last CLRF before the <code>block</code>
	 * included and computes the length of the overall record, returning it.
	 *
	 * @param hbuf the stream where to prebuffer
	 * @return the length of the record.
	 */
	private long prebufferHeader(FastByteArrayOutputStream hbuf) throws IOException {

		long dataLength;

		// warc header-line (staring from record-type included)

		hbuf.write(header.recordType.byteRepresentation);
		hbuf.write(' ');
		hbuf.write(BURL.toByteArray(header.subjectUri));
		hbuf.write(' ');
		hbuf.write(Util.getASCIIBytes(DATE_FORMAT.format(header.creationDate)));
		hbuf.write(' ');
		hbuf.write(header.contentType.byteRepresentation);
		hbuf.write(' ');
		hbuf.write(UUID_FIELD_NAME);
		hbuf.write(':');
		hbuf.write(Util.getASCIIBytes(header.recordId.toString()));
		hbuf.write(CRLF);

		// warc anvl-filed(s) (possibly empty, but to be CRLF terminated in any case)

		Util.writeANVLHeaders(hbuf, header.anvlFields, ANVL_CHARSET);
		hbuf.write(CRLF);

		/* compute overall size: prebuffered size + warc header-line (up to record-type excluded) + block size */

		// (6 = 2 blanks around size, and 2 ending crlf)
		dataLength =  (int)(WARC_ID.length + hbuf.length + block.length() + 6);

		// if d(x) are the decimal digits of x, it holds that d(x + d(x)) >= d(x) and if >, d(x + (d(x) + 1)) = (d(x) + 1)
		int digits = Util.digits(dataLength);
		dataLength += Util.digits(dataLength + digits) == digits ? digits : digits + 1;

		return dataLength;
	}

	/**
	 * Reads the <code>header-line</code> of the next warc record.
	 *
	 * <p> This method first positions the stream to the start of the next record (more precisely:
	 * if {@link #positionOfLastHeader} is not -1 it assumes to have been previously called on this
	 * same stream so that {@link #positionOfLastHeader} and {@link #dataLengthInLastHeader} are
	 * sensibly set).
	 *
	 * <p> It then reads the <code>header-line</code> (skipping the possible extra CRLFs before
	 * the <code>warc-id</code>) setting {@link #positionOfLastHeader} to the position in the
	 * stream of the first byte of the <code>header-line</code>.
	 *
	 * <p> Hence, it sets such line as the {@link #minimalisticParser} input and uses the parser to
	 * obtain the value of the <code>data-length</code> filed, that finally stores in
	 * {@link #dataLengthInLastHeader} (and returns).
	 *
	 * @param bin the stream where to read from.
	 * @return the value of the <code>data-length</code> field, or -1 if eof has been reached.
	 */
	private long readHeaderLine(FastBufferedInputStream bin) throws IOException, FormatException {

		// possibly consume the block part of a previous read

		if (positionOfLastHeader != -1) {
			long newPosition = positionOfLastHeader + dataLengthInLastHeader;
			if (ASSERTS) assert newPosition >= bin.position();
			if (USE_POSITION_INSTEAD_OF_SKIP)
				bin.position(newPosition);
			else
				bin.skip(newPosition - bin.position());
		}

		// read the header line

		int length = 0, read;
		byte[] gwowing = buffer;
		do {
			positionOfLastHeader = bin.position();

			while((read = bin.readLine(gwowing, length, gwowing.length - length, LINE_TERMINATOR)) == gwowing.length - length) {
				length += read;
				gwowing = ByteArrays.grow(gwowing, gwowing.length + 1);
			};
			if (read == -1) return -1;
			length += read;

			if (DEBUG) if (length >= 0) System.err.println("length: " + length + "\nline: " + Util.getString(buffer, 0, length));
		} while (length == 0);

		// set the parser to the read line

		minimalisticParser.setInput(gwowing, 0, length);

		// parse warc-id, parse data-length and assign it to dataLength

		minimalisticParser.positionAtNextWord();
		if (! minimalisticParser.startsWith(WARC_ID)) throw new FormatException("Missing or incorrect warc-id.");

		minimalisticParser.positionAtNextWord();
		dataLengthInLastHeader = minimalisticParser.asInt();

		return dataLengthInLastHeader;
	}

}
