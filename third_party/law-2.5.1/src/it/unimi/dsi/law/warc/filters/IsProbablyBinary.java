package it.unimi.dsi.law.warc.filters;

import java.io.IOException;
import java.io.InputStream;

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

import it.unimi.dsi.law.warc.util.HttpResponse;

// RELEASE-STATUS: DIST

/** Accepts only http responses whose content stream appears to be binary. */
public class IsProbablyBinary extends AbstractFilter<HttpResponse> {

	public static final IsProbablyBinary INSTANCE = new IsProbablyBinary();
	public static final int BINARY_CHECK_SCAN_LENGTH = 1000;
	/** The number of zeroes that must appear to cause the page to be considered probably
	 * binary. Some misconfigured servers emit one or two ASCII NULs at the start of their
	 * pages, so we use a relatively safe value. */
	public static final int THRESHOLD = 3;

	private IsProbablyBinary() {}

	/** This method implements a simple heuristic for guessing whether a page is binary.
	 *
	 * <P>The first {@link #BINARY_CHECK_SCAN_LENGTH} bytes are scanned: if we find more than
	 * {@link #THRESHOLD} zeroes, we deduce that this page is binary. Note that this works
	 * also with UTF-8, as no UTF-8 legal character encoding contains these characters (unless
	 * you're encoding 0, but this is not our case).
	 *
	 * @return <code>true</code> iff this page has most probably a binary content.
	 * @throws NullPointerException if the page has no byte content.
	 */
	public boolean apply(final HttpResponse httpResponse) {
		try {
			final InputStream content = httpResponse.contentAsStream();
			int count = 0;
			for(int i = BINARY_CHECK_SCAN_LENGTH; i-- != 0;) {
				final int b = content.read();
				if (b == -1) return false;
				if (b == 0 && ++count == THRESHOLD) return true;
			}
		}
		catch(IOException shouldntReallyHappen) {
			throw new RuntimeException(shouldntReallyHappen);
		}
		return false;
	}

	public static IsProbablyBinary valueOf(final String emptySpec) {
		if (emptySpec.length() > 0) throw new IllegalArgumentException();
		return INSTANCE;
	}

	public String toString() {
		return getClass().getSimpleName() + "()";
	}
}
