package it.unimi.dsi.law.util;

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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

import it.unimi.dsi.fastutil.longs.AbstractLong2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import it.unimi.dsi.util.XoRoShiRo128PlusRandomGenerator;

// RELEASE-STATUS: DIST

/**
 * Provides an implementation of consistent hashing. Consistent hashing has been introduced in
 * <blockquote> <P>Consistent Hashing and Random Trees: Distributed Caching Protocols for Relieving
 * Hot Spots on the World Wide Web, by David R. Karger, Eric Lehman, Frank T. Leighton, Rina
 * Panigrahy, Matthew S. Levine and Daniel Lewin, Proc. of the twenty-ninth annual ACM symposium on
 * Theory of computing, El Paso, Texas, United States, 1997, pages 654&minus;663. </blockquote> This
 * class provides some extension to the original definition: weighted buckets and skippable buckets.
 * More precisely, keys are distributed on buckets proportionally to the weight of the buckets, and
 * it is possible to specify a {@linkplain ConsistentHashFunction.SkipStrategy skip strategy} for
 * buckets.
 *
 * <H3>Consistent Hash Function: Properties</H3>
 *
 * <P>A consistent hash function consists, at any time, of a set of objects, called
 * <em>buckets</em>, each with a specified weight (a positive integer). At the beginning, there
 * are no buckets, but you can {@linkplain #add(Comparable, int) add a new bucket}, or
 * {@linkplain #remove(Comparable) remove an existing bucket}.
 *
 * <P>The method {@link #hash(long)} can be used to hash a <em>point</em> (a long) to a bucket.
 * More precisely, when applied to a given point, this method will return one of the buckets, and
 * the method will satisfy the following properties:
 *
 * <OL> <LI>the bucket returned by {@link #hash(long)} is one of the buckets currently present in
 * the consistent hash function; <LI>the fraction of points that are hashed to a specific bucket is
 * approximately proportional to the weight of the bucket; for example, if there are only two
 * buckets <var>A</var> and <var>B</var>, and the weight of <var>A</var> is 2 whereas the weight
 * of <var>B</var> is 3, then about 2/5 of all longs will be hashed to <var>A</var> and about 3/5
 * will be hashed to <var>B</var>; <LI>every time you add a new bucket, some of the points will of
 * course be hashed to the new bucket, but it is impossible that a point that was hashed to an old
 * bucket will now be hashed to some other old bucket; more formally, consider the following
 * sequence of instructions:
 *
 * <pre>
 * Object A = chf.hash(x);
 * chf.add(B);
 * Object C = chf.hash(x);
 * </pre>
 *
 * at the end either <code>A==C</code> (i.e., the hash of x has not changed after adding B), or
 * <code>C==B</code> (i.e., now x is hashed to the new object). </OL>
 *
 * <P>Otherwise said, if a new bucket is added, then the number of keys that change their
 * assignment is the minimum necessary; more importantly, it is impossible that a key changes its
 * bucket assignment towards a bucket that already existed: when a bucket is added old buckets can
 * only lose keys towards the new bucket.
 *
 * <P>It is easy to see that the last property stated above can be equivalently stated by saying
 * that every point determines a (total) order among buckets; the {@link #hash(long)} method only
 * returns the first element of this order. It is also possible, using {@link #hash(long, int)} to
 * obtain an <em>array</em> of buckets containing the (first part of the) order. In particular, if
 * the latter method is called with a specified length corresponding to the number of buckets, the
 * whole order will be returned.
 *
 * <H3>Implementation Details</H3>
 *
 * <P>With each bucket, we associate a number of points, called replicae, located on the unit
 * circle (the unit circle itself is represented approximately on the whole range of
 * <code>long</code>s). Then, given a point <var>p</var>, one can
 * {@linkplain #hash(long) get the bucket} corresponding to the replica that is closest to <var>p</var>.
 *
 * <P>The method that {@linkplain #hash(long, int) gets an array} containing the buckets looks at
 * the buckets that are closest to a point, in distance order, without repetitions. In particular,
 * by computing an array as large as the number of buckets, you will obtain a permutation of the
 * buckets themselves. Indeed, another viewpoint on consistent hashing is that it associates a
 * random permutation of the buckets to each point (albeit the interpretation of weights, in that
 * case, becomes a bit twisted).
 *
 * <P>The number of replicas associated to a bucket is fixed when the bucket is inserted in the
 * map. The actual number depends on the weight and on the constant {@link #REPLICAE_PER_BUCKET}.
 *
 * <P>This class handles overlaps (i.e., conflicts in a replica creation). In that case, a local
 * deterministic ordering is generated using the hash code (and, in case of a tie, the lexicographic
 * order of string representation) of the buckets.
 *
 * <P>The hashing function is deterministically based on the hash code of the buckets, which should
 * be of good quality. This is essential to ensure deterministic replicability of the results of
 * this function across different instances.
 *
 */

public final class ConsistentHashFunction<T extends Comparable<? super T>> {

	/** Each bucket is replicated this number of times. */
	public final static int REPLICAE_PER_BUCKET = 200;

	/** Maps points in the unit interval to buckets. */
	final protected Long2ObjectSortedMap<Object> replicae = new Long2ObjectAVLTreeMap<Object>();

	/** The cached key set of {@link #replicae}. */
	final protected ObjectSortedSet<it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry<Object>> entrySet = replicae.long2ObjectEntrySet();


	/** For each bucket, its size. */
	final protected Object2IntMap<T> sizes = new Object2IntOpenHashMap<T>();

	/** The cached key set of {@link #sizes}. */
	final protected Set<T> buckets = sizes.keySet();

	/** The optional strategy to skip buckets, or {@code null}. */
	final protected SkipStrategy<T> skipStrategy;

	private final static boolean DEBUG = false;

	/**
	 * Allows to skip suitable items when searching for the closest replica.
	 *
	 * <P>Sometimes it is useful to restrict the set of buckets that can be returned without
	 * modifying a consistent hash function (if not else, because any change requires removing or
	 * adding {@link ConsistentHashFunction#REPLICAE_PER_BUCKET} replicae).
	 *
	 * <P>To do so, it is possible to
	 * {@linkplain ConsistentHashFunction#ConsistentHashFunction(ConsistentHashFunction.SkipStrategy)
	 * provide at construction time} a strategy that, at each call to
	 * {@link ConsistentHashFunction#hash(long)}, will be used to test whether the bucket of a
	 * certain replica can be returned or not. Of course, in the latter case the search will
	 * continue with the next replica.
	 */

	public static interface SkipStrategy<T> {

		/**
		 * Checks whether a bucket can be returned or should be skipped.
		 *
		 * @param bucket the bucket to test.
		 * @return true if the bucket should be skipped.
		 */
		public boolean isSkippable(T bucket);
	}



	/** Creates a new consistent hash function. */
	public ConsistentHashFunction() {
		this(null);
	}

	/**
	 * Creates a new consistent hash function with given skip strategy.
	 *
	 * @param skipStrategy a skip strategy, or {@code null}.
	 */
	public ConsistentHashFunction(final SkipStrategy<T> skipStrategy) {
		this.skipStrategy = skipStrategy;
	}


	/**
	 * Adds a bucket to the map.
	 *
	 * @param bucket the new bucket.
	 * @param weight the weight of the new bucket; buckets with a larger weight are returned
	 * proportionately more often.
	 * @return false if the bucket was already present.
	 */



	@SuppressWarnings("unchecked")
	public boolean add(final T bucket, final int weight) {

		if (sizes.containsKey(bucket)) return false;
		sizes.put(bucket, weight);


		final XoRoShiRo128PlusRandomGenerator random = new XoRoShiRo128PlusRandomGenerator(bucket.hashCode());

		long point;
		Object o;
		SortedSet<T> conflictSet;

		for (int i = 0; i < weight * REPLICAE_PER_BUCKET; i++) {

			point = random.nextLong();
			if (DEBUG) point = point % 1024;

			if ((o = replicae.get(point)) != null) {
				if (o != bucket) { // o == bucket should happen with very low probability.
					if (o instanceof SortedSet) ((SortedSet<T>)o).add(bucket);
					else {
						if (DEBUG) System.err.println("Creating conflict set...");
						conflictSet = new TreeSet<T>();
						conflictSet.add((T)o);
						conflictSet.add(bucket);
						replicae.put(point, conflictSet);
					}
				}
			}
			else replicae.put(point, bucket);
		}

		return true;
	}

	/**
	 * Removes a bucket.
	 *
	 * @param bucket the bucket to be removed.
	 * @return false if the bucket was not present.
	 */

	@SuppressWarnings("unchecked")
	public boolean remove(final T bucket) {

		if (!sizes.containsKey(bucket)) return false;

		final XoRoShiRo128PlusRandomGenerator random = new XoRoShiRo128PlusRandomGenerator(bucket.hashCode());
		final int size = sizes.removeInt(bucket);

		long point;
		Object o;
		SortedSet<T> conflictSet;

		for (int i = 0; i < size * REPLICAE_PER_BUCKET; i++) {
			point = random.nextLong();

			if (DEBUG) point = point % 1024;
			o = replicae.remove(point);
			if (o instanceof SortedSet) {
				if (DEBUG) System.err.println("Removing from " + point + "  conflict set...");
				conflictSet = (SortedSet<T>)o;
				conflictSet.remove(bucket);
				if (conflictSet.size() > 1) replicae.put(point, conflictSet);
				else replicae.put(point, conflictSet.first());
			}
			else if (o != null && ((T)o).compareTo(bucket) != 0) replicae.put(point, o);
		}

		return true;
	}


	/**
	 * Returns an array of buckets whose replicae are close to the given point. The first element
	 * will be the bucket of the replica closest to the point, followed by the bucket of the next
	 * closest replica (whose bucket is not the first, of course) and so on.
	 *
	 * @param point a point on the unit circle.
	 * @param n the number of closest buckets to return.
	 * @return an array of distinct buckets of the closest replicas; the array could be shorter than
	 * <code>n</code> if there are not enough buckets and, in case a skip strategy has been
	 * specified, it could be empty even if the bucket set is nonempty.
	 */

	@SuppressWarnings("unchecked")
	public Object[] hash(long point, int n) {
		if (n == 0 || buckets.size() == 0) return ObjectArrays.EMPTY_ARRAY;

		if (DEBUG) point %= 1024;
		final ObjectLinkedOpenHashSet<Object> result = new ObjectLinkedOpenHashSet<Object>(n, .5f);

		ObjectBidirectionalIterator<it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry<Object>> i = replicae.long2ObjectEntrySet().iterator(new AbstractLong2ObjectSortedMap.BasicEntry<Object>(point, null));

		Object value;

		for (int pass = 2; pass-- != 0;) {
			while (i.hasNext()) {
				value = i.next().getValue();

				if (value instanceof SortedSet) {
					for (T p : (SortedSet<T>)value) {
						if ((skipStrategy == null || !skipStrategy.isSkippable(p)) && result.add(p) && --n == 0) return result.toArray();
					}
				}
				else if ((skipStrategy == null || !skipStrategy.isSkippable((T)value)) && result.add(value) && --n == 0) return result.toArray();
			}
			// Restart from the first element
			i = replicae.long2ObjectEntrySet().iterator();
		}

		return result.toArray();
	}

	/**
	 * Returns an array of buckets whose replicae are close to the given object.
	 *
	 * @param key an object ot hash.
	 * @param n the number of close buckets to return.
	 *
	 * <P>This method just uses <code>hashCode() << 32</code> as point for
	 * {@link #hash(long,int)}
	 * @return an array of distinct buckets of the closest replicas; the array could be shorter than
	 * <code>n</code> if there are not enough buckets and, in case a skip strategy has been
	 * specified, it could be empty even if the bucket set is nonempty.
	 * @see #hash(long, int)
	 */

	public Object[] hash(final Object key, final int n) {
		return hash((long)key.hashCode() << 32, n);
	}

	/**
	 * Returns the bucket of the replica that is closest to the given point.
	 *
	 * @param point a point on the unit circle.
	 * @return the bucket of the closest replica, or {@code null} if there are no buckets or
	 * all buckets must be skipped.
	 * @see #hash(long, int)
	 * @throws NoSuchElementException if there are no buckets, or if a skip strategy has been
	 * specified and it skipped all existings buckets.
	 */

	@SuppressWarnings("unchecked")
	public T hash(long point) {
		final Object result[] = hash(point, 1);
		if (result.length == 0) throw new NoSuchElementException();
		else return (T)result[0];
	}


	/**
	 * Returns the bucket of the replica that is closest to the given key.
	 *
	 * @param key an object to hash.
	 * @return the bucket of the closest replica, or {@code null} if there are no buckets or
	 * all buckets must be skipped.
	 * @see #hash(Object, int)
	 * @throws NoSuchElementException if there are no buckets, or if a skip strategy has been
	 * specified and it skipped all existings buckets.
	 */

	@SuppressWarnings("unchecked")
	public T hash(final Object key) {
		final Object result[] = hash(key, 1);
		if (result.length == 0) throw new NoSuchElementException();
		else return (T)result[0];
	}


	/**
	 * Returns the set of buckets of this consistent hash function.
	 *
	 * @return the set of buckets.
	 */

	public Set<T> buckets() {
		return buckets;
	}

	public String toString() {
		return replicae.toString();
	}
}
