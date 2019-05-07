package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2007-2017 Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import com.martiansoftware.jsap.ParseException;
import com.martiansoftware.jsap.stringparsers.ClassStringParser;

/** A small wrapper around JSAP's standard {@link ClassStringParser}. It
 * tries to prefix the package names in {@link #PACKAGE} to the provided
 * class name, making the specification of graph classes on the command line much easier. */

public class GraphClassParser extends ClassStringParser {
	/** The packages that will be prepended to each graph class. */
	public final static String[] PACKAGE = { "it.unimi.dsi.big.webgraph", "it.unimi.dsi.big.webgraph.labelling" };

	private final static GraphClassParser INSTANCE = new GraphClassParser();

	@SuppressWarnings("deprecation")
	protected GraphClassParser() {}

	public static ClassStringParser getParser() {
		return INSTANCE;
	}

	/** Parses the given class name, but as a first try prepends the package names found in {@link #PACKAGE}.
	 * @param className the name of a class, possibly without package specification.
	 */
	@Override
	public Object parse(String className) throws ParseException {
		for(String p: PACKAGE) {
			try {
				return super.parse(p + "." + className);
			}
			catch(Exception notFound) {}
		}
		return super.parse(className);
	}
}
