package it.unimi.dsi.law.warc.filters;

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

/** Accepts only fetched response whose status category (status/100) has a certain value.
 */
public class StatusCategory extends AbstractFilter<HttpResponse> {

	/** The accepted category (e.g., 2 for 2xx). */
	private final int category;

	/** Creates a filter that only accepts responses of the given category.
	 *
	 * @param category the accepted category.
	 */
	public StatusCategory(final int category) {
		this.category = category;
	}

	public boolean apply(HttpResponse x) {
		return x.status() / 100 == category;
	}

	public static StatusCategory valueOf(String spec) {
		return new StatusCategory(Integer.parseInt(spec));
	}

	public String toString() {
		return toString(String.valueOf(category));
	}

	public boolean equals(Object x) {
		if (x instanceof StatusCategory) return ((StatusCategory)x).category == category;
		else return false;
	}

}
