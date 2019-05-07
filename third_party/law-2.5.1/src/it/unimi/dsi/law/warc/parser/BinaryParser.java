package it.unimi.dsi.law.warc.parser;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/*
 * Copyright (C) 2012-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.law.warc.util.HttpResponse;
import it.unimi.dsi.law.warc.util.Response;

// RELEASE-STATUS: DIST

/** A universal binary parser that just computes digests. */

public class BinaryParser implements Parser {
	private final MessageDigest messageDigest;

	/** Builds a parser for digesting a page.
	 *
	 * @param messageDigest the digesting algorithm, or {@code null} if no digesting will be performed.
	 */
	public BinaryParser(final MessageDigest messageDigest) {
		this.messageDigest = messageDigest;
	}

	/** Builds a parser for digesting a page.
	 *
	 * @param messageDigestAlgorithm the digesting algorithm (as a string).
	 * @throws NoSuchAlgorithmException
	 */
	public BinaryParser(final String messageDigestAlgorithm) throws NoSuchAlgorithmException {
		this(MessageDigest.getInstance(messageDigestAlgorithm));
	}

	@Override
	public byte[] parse(final Response response, final LinkReceiver linkReceiver) throws IOException {
		if (messageDigest == null) return null;
		final HttpResponse httpResponse = (HttpResponse)response;
		final byte[] buffer = new byte[1024];
		InputStream is = httpResponse.contentAsStream();
		messageDigest.reset();
		for(int length; (length = is.read(buffer, 0, buffer.length)) > 0;) messageDigest.update(buffer, 0, length);
		return messageDigest.digest();
	}

	@Override
	public boolean apply(Response response) {
		return true;
	}

	@Override
	public Object clone() {
		return new BinaryParser(messageDigest);
	}

	@Override
	public String guessedCharset() {
		return null;
	}
}
