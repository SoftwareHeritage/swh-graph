package it.unimi.dsi.law.warc.util;

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
import it.unimi.dsi.law.warc.io.WarcRecord;



//RELEASE-STATUS: DIST

/** A mutable extension of {@link MetadataHttpResponse} that provides
 * support for {@linkplain #contentAsStream(MeasurableInputStream) setting the content stream}.
 * Note that {@link #fromWarcRecord(WarcRecord)} is not implemented. */
public class MutableHttpResponse extends MetadataHttpResponse {

	/** The content of this response. */
	private MeasurableInputStream contentAsStream;

	/** Sets the content.
	 *
	 * @param contentAsStream the content.
	 */
	public void contentAsStream(MeasurableInputStream contentAsStream) {
		this.contentAsStream = contentAsStream;
	}

	public MeasurableInputStream contentAsStream() throws IOException {
		if (contentAsStream == null) throw new IllegalStateException("The content stream has not been set yet");
		return contentAsStream;
	}

	public boolean fromWarcRecord(WarcRecord wr) throws IOException {
		throw new UnsupportedOperationException();
	}
}
