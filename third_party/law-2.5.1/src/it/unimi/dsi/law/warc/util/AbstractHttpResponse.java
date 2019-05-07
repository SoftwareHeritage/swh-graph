package it.unimi.dsi.law.warc.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.UUID;

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

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.law.warc.io.MeasurableSequenceInputStream;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord.ContentType;
import it.unimi.dsi.law.warc.io.WarcRecord.RecordType;
import it.unimi.dsi.util.XorShift128PlusRandomGenerator;

// RELEASE-STATUS: DIST

/** An abstract implementation of {@link HttpResponse} providing a {@link #toWarcRecord(WarcRecord)} method that can
 *  be used to populate a WARC record (in order to write it). */

public abstract class AbstractHttpResponse implements HttpResponse {

	/** A high-quality pseudorandom generator to generate UUIDs. */
	protected final XorShift128PlusRandomGenerator random = new XorShift128PlusRandomGenerator();

	/**
	 * Populates a WARC record with contents from this response.
	 *
	 * <p>This method uses the getters of the {@link HttpResponse} interface to populate the given record.
	 * For this reason, concrete implementations of this class must provide an implementation for
	 * {@link HttpResponse#contentAsStream()} that will be used to setup the {@link WarcRecord#block}.
	 *
	 * <p>Moreover, if the concrete implementation through which this method is called implements the
	 * {@link DigestBasedDuplicateDetection} interface, the WARC record will be also populated with the
	 * information required to represent the duplicate information.
	 *
	 * @param record the record.
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	@SuppressWarnings("resource")
	public void toWarcRecord(WarcRecord record) throws UnsupportedEncodingException, IOException {

		// fill header

		WarcRecord.Header header = record.header;

		header.recordType = RecordType.RESPONSE;
		header.subjectUri = uri();
		header.creationDate = new Date();

		String scheme = header.subjectUri.getScheme();
		if (scheme == null) throw new IllegalArgumentException("No scheme avaialbe for " + header.subjectUri);
		if (scheme.equals("https"))	header.contentType = ContentType.HTTPS;
		else if (scheme.equals("http"))	header.contentType = ContentType.HTTP;
		else throw new IllegalArgumentException("Only http/https schemes allowed, instead scheme is " + scheme);

		header.recordId = new UUID(random.nextLong(), random.nextLong());
		header.anvlFields.clear();

		// fill block with headers only

		FastByteArrayOutputStream buf = new FastByteArrayOutputStream();

		assert statusLine() != null;
		assert statusLine().toString() != null;

		buf.write(statusLine().toString().getBytes(HEADER_CHARSET));
		buf.write(WarcRecord.CRLF);
		Util.writeANVLHeaders(buf, headers(), HEADER_CHARSET);
		buf.write(WarcRecord.CRLF);
		buf.flush();
		final MeasurableInputStream headerBlock = new FastByteArrayInputStream(buf.array, 0, buf.length);

		// deal with block according to duplicate information

		if (this instanceof DigestBasedDuplicateDetection) {
			DigestBasedDuplicateDetection t = (DigestBasedDuplicateDetection)this;
			if (t.digest() != null) record.header.anvlFields.put(HttpResponse.DIGEST_HEADER, Util.toHexString(t.digest()));
			if (t.isDuplicate()) {
				record.header.anvlFields.put(HttpResponse.ISDUPLICATE_HEADER, "true");
				record.block = headerBlock;
				return;
			}
		}

		// append content

		record.block = new MeasurableSequenceInputStream(headerBlock, contentAsStream());
	}
}
