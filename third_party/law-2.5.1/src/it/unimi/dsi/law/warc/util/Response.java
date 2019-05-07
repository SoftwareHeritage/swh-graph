package it.unimi.dsi.law.warc.util;

import java.io.IOException;
import java.net.URI;

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

import it.unimi.dsi.law.warc.io.WarcRecord;

// RELEASE-STATUS: DIST

/** Provides high level access to WARC records with <code>record-type</code> equal to
 * <code>response</code>.
 */
public interface Response {

	/** Returns the URI associated with this response.
	 *
	 * @return the URI associated with this response.
	 */
	URI uri();

	/** Fills this response with the content of a {@link WarcRecord} (optional operation).
	 *
	 * @param record the record.
	 * @return true iff the  <code>record-type</code> of the given record is <code>response</code>.
	 * @throws IOException
	 */
	public boolean fromWarcRecord(WarcRecord record) throws IOException;

}
