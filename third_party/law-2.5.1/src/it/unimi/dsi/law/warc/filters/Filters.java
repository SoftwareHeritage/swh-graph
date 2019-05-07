package it.unimi.dsi.law.warc.filters;

import java.lang.reflect.Method;
import java.net.URI;

import org.apache.commons.lang.StringUtils;

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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.law.warc.filters.parser.ParseException;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.util.HttpResponse;

// RELEASE-STATUS: DIST

/** A collection of static methods to deal with {@link Filter filters}. */
public class Filters {

	public static final Filter<?>[] EMPTY_ARRAY = {};

	/** A set containing all filters in the bubing.filters package. This primitive technique
	 * is used to circumvent the impossibility of obtaining all classes in a package by reflection. */
	@SuppressWarnings("unchecked")
	private static final ObjectOpenHashSet<Class<? extends Filter<?>>> FILTERS = new ObjectOpenHashSet<Class<? extends Filter<?>>>(
			// TODO: periodically check that this list is complete.
			new Class[] { ContentTypeStartsWith.class, DigestEquals.class, DuplicateSegmentsLessThan.class,
					HostEndsWith.class, HostEquals.class, IsHttpResponse.class, PathEndsWithOneOf.class,
					SchemeEquals.class, StatusCategory.class, URLEquals.class,
					URLMatchesRegex.class, URLShorterThan.class, IsProbablyBinary.class
					}
			);

	/** Produces the conjunction of the given filters.
	 *
	 * @param <T> the type of objects that the filters deal with.
	 * @param f the filters.
	 * @return the conjunction.
	 */
	@SafeVarargs
	public static<T> Filter<T> and(final Filter<T>... f) {
		return new Filter<T>() {
			public boolean apply(final T x) {
				for (Filter<T> filter: f) if (! filter.apply(x)) return false;
				return true;
			}

			public String toString() {
				return "(" + StringUtils.join(f, " and ") + ")";
			}
		};
	}

	/** Produces the disjunction of the given filters.
	 *
	 * @param <T> the type of objects that the filters deal with.
	 * @param f the filters.
	 * @return the disjunction.
	 */
	@SafeVarargs
	public static<T> Filter<T> or(final Filter<T>... f) {
		return new Filter<T>() {
			public boolean apply(final T x) {
				for (Filter<T> filter: f) if (filter.apply(x)) return true;
				return false;
			}

			public String toString() {
				return "(" + StringUtils.join(f, " or ") + ")";
			}

		};
	}

	/** Produces the negation of the given filter.
	 *
	 * @param <T> the type of objects that the filter deal with.
	 * @param filter the filter.
	 * @return the negation of the given filter.
	 */
	public static<T> Filter<T> not(final Filter<T> filter) {
		return new AbstractFilter<T>() {
			public boolean apply(final T x) {
				return ! filter.apply(x);
			}

			public String toString() {
				return "(not " + filter + ")";
			}
		};
	}

	// TODO: change this to a static, correctly typed method.
	/** The constantly true filter. */
	@SuppressWarnings("rawtypes")
	public static Filter TRUE = new Filter() {
		public boolean apply(Object x) {
			return true;
		}

		public String toString() {
			return "true";
		}
	};

	@SuppressWarnings("rawtypes")
	/** The constantly false filter. */
	public static Filter FALSE = new Filter() {
		public boolean apply(Object x) {
			return false;
		}

		public String toString() {
			return "false";
		}
	};


	/** Creates a filter from a filter class name and an external form.
	 *
	 * @param className the name of a filter class; it may either be a single class name (in which case it
	 * 	will be qualified with {@link Filter#FILTER_PACKAGE_NAME}) or a fully qualified classname.
	 * @param spec the specification from which the filter will be created, using the <tt>valueOf(String)</tt> method (see {@link Filter}).
	 * @param tClass the base class of the filter that is desired: it should coincide with <code>T</code>; if the base type <code>D</code> of
	 *  the filter is wrong, it will try to adapt it by using a static method in the Filters class whose signature is
	 *  <pre>public static Filter&lt;T&gt; adaptD2T(Filter&lt;D&gt;)</pre>.
	 * @return the filter.
	 */
	@SuppressWarnings("unchecked")
	public static<T> Filter<T> getFilterFromSpec(String className, String spec, Class<T> tClass) throws ParseException {
		String filterClassName;

		if (className.indexOf('.') >= 0) filterClassName = className;
		else filterClassName = Filter.FILTER_PACKAGE_NAME + "." + className;
		try {
			// Produce the filter
			Class<?> c = Class.forName(filterClassName);
			if (! Filter.class.isAssignableFrom(c)) throw new ParseException(filterClassName + " is not a valid filter class");
			Filter<T> filter = (Filter<T>)c.getMethod("valueOf", String.class).invoke(null, spec);

			// Extract its base type
			final Method method[] = filter.getClass().getMethods();
			int i;
			for (i = 0; i < method.length; i++) if (! method[i].isSynthetic() && method[i].getName().equals("apply")) break;
			if (i == method.length) throw new NoSuchMethodException("Could not find apply method in filter " + filter);
			final Class<?>[] parameterTypes = method[i].getParameterTypes();
			if (parameterTypes.length != 1) throw new NoSuchMethodException("Could not find one-argument apply method in filter " + filter);
			final Class<?> toClass = parameterTypes[0];

			// Possibly: adapt the filter
			if (toClass.equals(tClass)) return filter;
			else {
				Method adaptMethod;
				try {
					adaptMethod = Filters.class.getMethod("adaptFilter" + toClass.getSimpleName() + "2" + tClass.getSimpleName(), Filter.class);
				} catch (NoSuchMethodException e) {
					throw new NoSuchMethodException("Cannot adapt a Filter<" + toClass.getSimpleName() + "> into Filter<" + tClass.getSimpleName() + ">");
				}
				return (Filter<T>)adaptMethod.invoke(null, filter);
			}
		}
		catch(ParseException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/** Adapts a filter with {@link String} base type to a filter with {@link URI} base type. For testing purposes only.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<URI> adaptFilterString2URI(final Filter<String> original) {
		return new AbstractFilter<URI>() {
			public boolean apply(final URI uri) {
				return original.apply(uri.toString());
			}
			public String toString() {
				return original.toString();
			}
		};
	}

	/** Adapts a filter with {@link URI} base type to a filter with {@link HttpResponse} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<HttpResponse> adaptFilterURI2HttpResponse(final Filter<URI> original) {
		return new AbstractFilter<HttpResponse>() {
			public boolean apply(final HttpResponse response) {
				return original.apply(response.uri());
			}
			public String toString() {
				return original.toString();
			}
		};
	}

	/** Adapts a filter with {@link URI} base type to a filter with {@link WarcRecord} base type.
	 *
	 * @param original the original filter.
	 * @return the adapted filter.
	 */
	public static Filter<WarcRecord> adaptFilterURI2WarcRecord(final Filter<URI> original) {
		return new AbstractFilter<WarcRecord>() {
			public boolean apply(WarcRecord x) {
				return original.apply(x.header.subjectUri);
			}
			public String toString() {
				return original.toString();
			}
		};
	}

	/** Returns a list of the standard filter classes.
	 *
	 * @return a list of standard filter classes.
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends Filter<?>>[] standardFilters() {
		return FILTERS.toArray(new Class[FILTERS.size()]);
	}

}
