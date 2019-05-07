package it.unimi.dsi.law.warc.parser;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

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

import it.unimi.dsi.fastutil.objects.ObjectSets;
import it.unimi.dsi.law.warc.filters.Filter;
import it.unimi.dsi.law.warc.util.Response;

// RELEASE-STATUS: DIST

/** A generic parser for {@link Response responses}. It provides link extraction through a
 * {@link LinkReceiver} callback and optional digesting.
 */

public interface Parser extends Filter<Response>, Cloneable {
	/** A class that can receive URLs discovered during parsing. It may be used to
	 *  iterate over the URLs found in the current page, but what will be actually
	 *  returned by the iterator is implementation-dependent. */
	public static interface LinkReceiver extends Iterable<URI> {
		/** Handles the location defined by headers.
		 *
		 * @param location the location defined by headers.
		 */
		public void location(URI location);
		/** Handles the location defined by a <code>META</code> element.
		 *
		 * @param location the location defined by the <code>META</code> element.
		 */
		public void metaLocation(URI location);
		/** Handles the refresh defined by a <code>META</code> element.
		 *
		 * @param refresh the URL defined by the <code>META</code> element.
		 */
		public void metaRefresh(URI refresh);
		/** Handles a link.
		 *
		 * @param uri a link discovered during the parsing phase.
		 */
		public void link(URI uri);
		/** Initializes this receiver for a new page.
		 *
		 * @param responseUrl the URL of the page to be parsed.
		 */
		public void init(URI responseUrl);
	}

	/** A no-op implementation of {@link LinkReceiver}. */
	public final static LinkReceiver NULL_LINK_RECEIVER = new LinkReceiver() {
		@Override
		public void location(URI location) {}
		@Override
		public void metaLocation(URI location) {}
		@Override
		public void metaRefresh(URI refresh) {}
		@Override
		public void link(URI link) {}
		@Override
		public void init(URI responseUrl) {}
		@SuppressWarnings("unchecked")
		@Override
		public Iterator<URI> iterator() { return ObjectSets.EMPTY_SET.iterator(); }
	};

	/** Parses a response.
	 *
	 * @param response a response to parse.
	 * @param linkReceiver a link receiver.
	 * @return a byte digest for the page, or {@code null} if no digest has been computed.
	 */
	public byte[] parse(Response response, LinkReceiver linkReceiver) throws IOException;

	/** Returns a guessed charset for the document, or {@code null} if the charset could
	 *  not be guessed.
	 *
	 * @return a charset or {@code null}.
	 */
	public String guessedCharset();
}
