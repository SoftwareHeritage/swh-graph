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

/** An abstract implementation of a lazy integer iterator, implementing {@link #skip(long)}
 * by repeated calls to {@link LazyLongIterator#nextLong() nextInt()}. */

public abstract class AbstractLazyLongIterator implements LazyLongIterator {

	@Override
	public long skip(final long n) {
		long i;
		for(i = 0; i < n && nextLong() != -1; i++);
		return i;
	}

}
