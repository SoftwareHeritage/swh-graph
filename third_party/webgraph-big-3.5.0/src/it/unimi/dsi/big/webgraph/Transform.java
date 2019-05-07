package it.unimi.dsi.big.webgraph;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/*
 * Copyright (C) 2003-2017 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.Label;
import it.unimi.dsi.big.webgraph.labelling.LabelMergeStrategy;
import it.unimi.dsi.big.webgraph.labelling.LabelSemiring;
import it.unimi.dsi.big.webgraph.labelling.Labels;
import it.unimi.dsi.big.webgraph.labelling.UnionArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.BigSwapper;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongHeapSemiIndirectPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XorShift1024StarRandom;

/** Static methods that manipulate immutable graphs.
 *
 *  <P>Most methods take an {@link
 *  it.unimi.dsi.big.webgraph.ImmutableGraph} (along with some other data, that
 *  depend on the kind of transformation), and return another {@link
 *  it.unimi.dsi.big.webgraph.ImmutableGraph} that represents the transformed
 *  version.
 */

public class Transform {
	private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);

	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	private static final boolean ASSERTS = false;

	private Transform() {}


	/** Provides a method to accept or reject an arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface ArcFilter {

		/**
		 * Tells if the arc <code>(i,j)</code> has to be accepted or not.
		 *
		 * @param i the source of the arc.
		 * @param t the destination of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept(long i, long t);
	}

	/** Provides a method to accept or reject a labelled arc.
	 *
	 * <P>Note that arc filters are usually stateless. Thus, their declaration
	 * should comprise a static singleton (e.g., {@link Transform#NO_LOOPS}).
	 */
	public interface LabelledArcFilter {

		/**
		 * Tells if the arc <code>(i,j)</code> with label <code>label</code> has to be accepted or not.
		 *
		 * @param i the source of the arc.
		 * @param j the destination of the arc.
		 * @param label the label of the arc.
		 * @return if the arc has to be accepted.
		 */
		public boolean accept(long i, long j, Label label);
	}

	/** An arc filter that rejects loops. */
	final static private class NoLoops implements ArcFilter, LabelledArcFilter {
		private NoLoops() {}
		/** Returns true if the two arguments differ.
		 *
		 * @return <code>i != j</code>.
		 */
		@Override
		public boolean accept(final long i, final long j) {
			return i != j;
		}
		@Override
		public boolean accept(long i, long j, Label label) {
			return i != j;
		}
	}

	/** An arc filter that only accepts arcs whose endpoints belong to the same
	 * (if the parameter <code>keepOnlySame</code> is true) or to
	 *  different (if <code>keepOnlySame</code> is false) classes.
	 *  Classes are specified by one long per node, read from a given file in {@link DataInput} format. */
	public final static class NodeClassFilter implements ArcFilter, LabelledArcFilter {
		private final boolean keepOnlySame;
		private final long[][] nodeClass;

		/** Creates a new instance.
		 *
		 * @param classFile name of the class file.
		 * @param keepOnlySame whether to keep nodes in the same class.
		 */
		public NodeClassFilter(final String classFile, final boolean keepOnlySame) {
			try {
				nodeClass = BinIO.loadLongsBig(classFile);
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
			this.keepOnlySame = keepOnlySame;
		}

		/** Creates a new instance.
		 *
		 * <p>This constructor has the same arguments as {@link #NodeClassFilter(String,boolean)},
		 * but it can be used with an {@link ObjectParser}.
		 *
		 * @param classFile name of the class file.
		 * @param keepOnlySame whether to keep nodes in the same class.
		 */
		public NodeClassFilter(String classFile, String keepOnlySame) {
			this(classFile, Boolean.parseBoolean(keepOnlySame));
		}

		@Override
		public boolean accept(final long i, final long j) {
			return keepOnlySame == (LongBigArrays.get(nodeClass, i) == LongBigArrays.get(nodeClass, j));
		}

		@Override
		public boolean accept(long i, long j, Label label) {
			return keepOnlySame == (LongBigArrays.get(nodeClass, i) == LongBigArrays.get(nodeClass, j));
		}
	}

	/** An arc filter that rejects arcs whose well-known attribute has a value smaller than a given threshold. */
	final static public class LowerBound implements LabelledArcFilter {
		private final int lowerBound;

		public LowerBound(final int lowerBound) {
			this.lowerBound = lowerBound;
		}

		public LowerBound(String lowerBound) {
			this(Integer.parseInt(lowerBound));
		}
		/** Returns true if the integer value associated to the well-known attribute of the label is larger than the threshold.
		 *
		 * @return true if <code>label.{@link Label#getInt()}</code> is larger than the threshold.
		 */
		@Override
		public boolean accept(long i, long j, Label label) {
			return label.getInt() >= lowerBound;
		}
	}


	/** A singleton providing an arc filter that rejects loops. */
	final static public NoLoops NO_LOOPS = new NoLoops();

	/** A class that exposes an immutable graph viewed through a filter. */
	private static final class FilteredImmutableGraph extends ImmutableGraph {
		private final ArcFilter filter;
		private final ImmutableGraph graph;
		private long[][] succ;
		private long currentNode = -1;

		private FilteredImmutableGraph(ArcFilter filter, ImmutableGraph graph) {
			this.filter = filter;
			this.graph = graph;
		}

		@Override
		public long numNodes() {
			return graph.numNodes();
		}

		@Override
		public FilteredImmutableGraph copy() {
			return new FilteredImmutableGraph(filter, graph.copy());
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		@Override
		public LazyLongIterator successors(final long x) {
			return new AbstractLazyLongIterator() {

				@SuppressWarnings("hiding")
				private final LazyLongIterator succ = graph.successors(x);

				@Override
				public long nextLong() {
					long t;
					while ((t = succ.nextLong()) != -1) if (filter.accept(x, t)) return t;
					return -1;
				}
			};
		}

		@Override
		public long[][] successorBigArray(long x) {
			if (currentNode != x) {
				succ = LazyLongIterators.unwrap(successors(x));
				currentNode = x ;
			}
			return succ;
		}

		@Override
		public long outdegree(long x) {
			if (currentNode != x) {
				succ = successorBigArray(x);
				currentNode = x;
			}
			return LongBigArrays.length(succ);
		}

		@Override
		public NodeIterator nodeIterator() {
			return new NodeIterator() {
				final NodeIterator nodeIterator = graph.nodeIterator();
				@SuppressWarnings("hiding")
				long[][] succ = LongBigArrays.EMPTY_BIG_ARRAY;
				long outdegree = -1;

				@Override
				public long outdegree() {
					if (outdegree == -1) throw new IllegalStateException();
					return outdegree;
				}

				@Override
				public long nextLong() {
					final long currNode = nodeIterator.nextLong();
					final long oldOutdegree = nodeIterator.outdegree();
					LazyLongIterator oldSucc = nodeIterator.successors();
					succ = LongBigArrays.ensureCapacity(succ, oldOutdegree, 0);
					outdegree = 0;
					for(long i = 0; i < oldOutdegree; i++) {
						final long s = oldSucc.nextLong();
						if (filter.accept(currNode, s)) LongBigArrays.set(succ, outdegree++, s);
					}
					return currNode;
				}

				@Override
				public long[][] successorBigArray() {
					if (outdegree == -1) throw new IllegalStateException();
					return succ;
				}


				@Override
				public boolean hasNext() {
					return nodeIterator.hasNext();
				}
			};
		}
	}

	/** A class that exposes an arc-labelled immutable graph viewed through a filter. */
	private static final class FilteredArcLabelledImmutableGraph extends ArcLabelledImmutableGraph {
		private final LabelledArcFilter filter;
		private final ArcLabelledImmutableGraph graph;
		private long[][] succ;
		private Label[][] label;
		private long currentNode = -1;

		private final class FilteredLabelledArcIterator extends AbstractLazyLongIterator implements LabelledArcIterator {
			private final long x;

			private final LabelledArcIterator successors;

			private FilteredLabelledArcIterator(final long x, final LabelledArcIterator successors) {
				this.x = x;
				this.successors = successors;
			}

			@Override
			public long nextLong() {
				long t;
				while ((t = successors.nextLong()) != -1) if (filter.accept(x, t, successors.label())) return t;
				return -1;
			}

			@Override
			public Label label() {
				return successors.label();
			}
		}

		private FilteredArcLabelledImmutableGraph(LabelledArcFilter filter, ArcLabelledImmutableGraph graph) {
			this.filter = filter;
			this.graph = graph;
		}

		@Override
		public long numNodes() {
			return graph.numNodes();
		}

		@Override
		public ArcLabelledImmutableGraph copy() {
			return new FilteredArcLabelledImmutableGraph(filter, graph.copy());
		}

		@Override
		public boolean randomAccess() {
			return graph.randomAccess();
		}

		@Override
		public Label prototype() {
			return graph.prototype();
		}

		@Override
		public LabelledArcIterator successors(final long x) {
			return new FilteredLabelledArcIterator(x, graph.successors(x));
		}

		@Override
		public long[][] successorBigArray(final long x) {
			if (currentNode != x) outdegree(x); // Just to fill the cache
			return succ;
		}

		@Override
		public Label[][] labelBigArray(final long x) {
			if (currentNode != x) outdegree(x); // Just to fill the cache
			return label;
		}

		@Override
		public long outdegree(final long x) {
			if (currentNode != x) {
				succ = super.successorBigArray(x);
				label = super.labelBigArray(x);
				currentNode = x;
			}
			return LongBigArrays.length(succ);
		}

		@Override
		public ArcLabelledNodeIterator nodeIterator() {
			return new ArcLabelledNodeIterator() {
				final ArcLabelledNodeIterator nodeIterator = graph.nodeIterator();
				private long currNode = -1;
				private long outdegree = -1;

				@Override
				public long outdegree() {
					if (currNode == -1) throw new IllegalStateException();
					if (outdegree == -1) {
						long d = 0;
						LabelledArcIterator successors = successors();
						while(successors.nextLong() != -1) d++;
						outdegree = d;
					}
					return outdegree;
				}

				@Override
				public long nextLong() {
					outdegree = -1;
					return currNode = nodeIterator.nextLong();
				}

				@Override
				public boolean hasNext() {
					return nodeIterator.hasNext();
				}

				@Override
				public LabelledArcIterator successors() {
					return new FilteredLabelledArcIterator(currNode, nodeIterator.successors());
				}
			};
		}

	}

	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger, which will be ignored.
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs(final ImmutableGraph graph, final ArcFilter filter, final ProgressLogger ignored) {
		return filterArcs(graph, filter);
	}

	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @param ignored a progress logger, which will be ignored.
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter, final ProgressLogger ignored) {
		return filterArcs(graph, filter);
	}

	/** Returns a graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ImmutableGraph filterArcs(final ImmutableGraph graph, final ArcFilter filter) {
		return new FilteredImmutableGraph(filter, graph);
	}

	/** Returns a labelled graph with some arcs eventually stripped, according to the given filter.
	 *
	 * @param graph a labelled graph.
	 * @param filter the filter (telling whether each arc should be kept or not).
	 * @return the filtered graph.
	 */
	public static ArcLabelledImmutableGraph filterArcs(final ArcLabelledImmutableGraph graph, final LabelledArcFilter filter) {
		return new FilteredArcLabelledImmutableGraph(filter, graph);
	}


	/** Returns a symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return symmetrizeOffline(g, batchSize, null, null);
	}

	/** Returns a symmetrized graph using an offline transposition.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return the symmetrized graph.
	 * @see #symmetrizeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return symmetrizeOffline(g, batchSize, tempDir, null);
	}

	/** Returns a symmetrized graph using an offline transposition.
	 *
	 * <P>The symmetrized graph is the union of a graph and of its transpose. This method will
	 * compute the transpose on the fly using {@link #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)}.
	 *
	 * @param g the source graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return the symmetrized graph.
	 */
	public static ImmutableGraph symmetrizeOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		return union(g, transposeOffline(g, batchSize, tempDir, pl));
	}

	/* Provides a sequential immutable graph by merging batches on the fly. */
	public final static class BatchGraph extends ImmutableSequentialGraph {
		private final ObjectArrayList<File> batches;
		private final long n;
		private final long numArcs;

		public BatchGraph(final long n, final long m, final ObjectArrayList<File> batches) {
			this.batches = batches;
			this.n = n;
			this.numArcs = m;
		}

		@Override
		public long numNodes() { return n; }
		@Override
		public long numArcs() {
			if (numArcs == -1) throw new UnsupportedOperationException();
			return numArcs;
		}

		@Override
		public BatchGraph copy() {
			return this;
		}

		@Override
		public NodeIterator nodeIterator(long from) {
			if (from != 0) throw new UnsupportedOperationException();
			else return nodeIterator();
		}

		@Override
		public NodeIterator nodeIterator() {
			final long[] refArray = new long[batches.size()];
			final InputBitStream[] batchIbs = new InputBitStream[refArray.length];
			final int[] inputStreamLength = new int[refArray.length];
			final long[] prevTarget = new long[refArray.length];
			Arrays.fill(prevTarget, -1);
			// The indirect queue used to merge the batches.
			final LongHeapSemiIndirectPriorityQueue queue = new LongHeapSemiIndirectPriorityQueue(refArray);

			try {
				// We open all files and load the first element into the reference array.
				for(int i = 0; i < refArray.length; i++) {
					batchIbs[i] = new InputBitStream(batches.get(i));
					try {
						inputStreamLength[i] = batchIbs[i].readDelta();
						refArray[i] = batchIbs[i].readLongDelta();
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}

					queue.enqueue(i);
				}

				return new NodeIterator() {
					/** The last returned node. */
					private long last = -1;
					/** The outdegree of the current node (valid if {@link #last} is not -1). */
					private long outdegree;
					/** The successors of the current node (valid if {@link #last} is not -1);
					 * only the first {@link #outdegree} entries are meaningful. */
					private long[][] successor = LongBigArrays.EMPTY_BIG_ARRAY;

					@Override
					public long outdegree() {
						if (last == -1) throw new IllegalStateException();
						return outdegree;
					}

					@Override
					public boolean hasNext() {
						return last < n - 1;
					}

					@Override
					public long nextLong() {
						last++;
						long d = 0;
						int i;

						try {
							/* We extract elements from the queue as long as their target is equal
							 * to last. If during the process we exhaust a batch, we close it. */

							while(! queue.isEmpty() && refArray[i = queue.first()] == last) {
								successor = LongBigArrays.grow(successor, d + 1);
								LongBigArrays.set(successor, d, prevTarget[i] += batchIbs[i].readLongDelta() + 1);
								if (--inputStreamLength[i] == 0) {
									queue.dequeue();
									batchIbs[i].close();
									batchIbs[i] = null;
								}
								else {
									// We read a new source and update the queue.
									final long sourceDelta = batchIbs[i].readLongDelta();
									if (sourceDelta != 0) {
										refArray[i] += sourceDelta;
										prevTarget[i] = -1;
										queue.changed();
									}
								}
								d++;
							}
							// Neither quicksort nor heaps are stable, so we reestablish order here.
							LongBigArrays.quickSort(successor, 0, d);
							if (d != 0) {
								long p = 0;
								long pSuccessor = LongBigArrays.get(successor, p);

								for(long j = 1; j < d; j++) {
									final long s = LongBigArrays.get(successor, j);
									if (pSuccessor != s) {
										LongBigArrays.set(successor, ++p, s);
										pSuccessor = s;
									}
								}
								d = p + 1;
							}
						}
						catch(IOException e) {
							throw new RuntimeException(e);
						}

						outdegree = d;
						return last;
					}

					@Override
					public long[][] successorBigArray() {
						if (last == -1) throw new IllegalStateException();
						return successor;
					}

					@Override
					protected void finalize() throws Throwable {
						try {
							for(InputBitStream ibs: batchIbs) if (ibs != null) ibs.close();
						}
						finally {
							super.finalize();
						}
					}


				};
			}
			catch(IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		protected void finalize() throws Throwable {
			try {
				for(File f : batches) f.delete();
			}
			finally {
				super.finalize();
			}
		}

	};

	/** Sorts the given source and target arrays w.r.t. the target and stores them in a temporary file.
	 *
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param tempDir a temporary directory where to store the sorted arrays, or <code>null</code>
	 * @param batches a list of files to which the batch file will be added.
	 * @return the number of pairs in the batch (might be less than <code>n</code> because duplicates are eliminated).
	 */

	public static int processBatch(final int n, final long[] source, final long[] target, final File tempDir, final List<File> batches) throws IOException {

		LongArrays.parallelQuickSort(source, target, 0, n);

		final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
		batchFile.deleteOnExit();
		batches.add(batchFile);
		final OutputBitStream batch = new OutputBitStream(batchFile);
		int u = 0;
		if (n != 0) {
			// Compute unique pairs
			u = 1;
			for(int i = n - 1; i-- != 0;) if (source[i] != source[i + 1] || target[i] != target[i + 1]) u++;
			batch.writeDelta(u);
			long prevSource = source[0];
			batch.writeLongDelta(prevSource);
			batch.writeLongDelta(target[0]);

			for(int i = 1; i < n; i++) {
				if (source[i] != prevSource) {
					batch.writeLongDelta(source[i] - prevSource);
					batch.writeLongDelta(target[i]);
					prevSource = source[i];
				}
				else if (target[i] != target[i - 1]) {
					// We don't write duplicate pairs
					batch.writeDelta(0);
					if (ASSERTS) assert target[i] > target[i - 1] : target[i] + "<=" + target[i - 1];
					batch.writeLongDelta(target[i] - target[i - 1] - 1);
				}
			}
		}
		else batch.writeDelta(0);

		batch.close();
		return u;
	}

	/** Sorts the given source and target arrays w.r.t. the target and stores them in two temporary files.
	 *  An additional positionable input bit stream is provided that contains labels, starting at given positions.
	 *  Labels are also written onto the appropriate file.
	 *
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param start the array containing the bit position (within the given input stream) where the label of the arc starts.
	 * @param labelBitStream the positionable bit stream containing the labels.
	 * @param tempDir a temporary directory where to store the sorted arrays.
	 * @param batches a list of files to which the batch file will be added.
	 * @param labelBatches a list of files to which the label batch file will be added.
	 */

	private static void processTransposeBatch(final int n, final long[] source, final long[] target, final long[] start,
			final InputBitStream labelBitStream, final File tempDir, final List<File> batches, final List<File> labelBatches,
			final Label prototype) throws IOException {
		it.unimi.dsi.fastutil.Arrays.quickSort(0, n, new IntComparator() {
			@Override
			public int compare(int x, int y) {
				long t = source[x] - source[y];
				if (t != 0) return t < 0 ? -1 : 1;
				t = target[x] - target[y];
				return t == 0 ? 0 : t < 0 ? -1 : 1;
			}
		}, new Swapper() {
			@Override
			public void swap(int x, int y) {
				long t = source[x];
				source[x] = source[y];
				source[y] = t;
				t = target[x];
				target[x] = target[y];
				target[y] = t;
				long u = start[x];
				start[x] = start[y];
				start[y] = u;
			}
		});

		final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
		batchFile.deleteOnExit();
		batches.add(batchFile);
		final OutputBitStream batch = new OutputBitStream(batchFile);

		if (n != 0) {
			// Compute unique pairs
			batch.writeDelta(n);
			long prevSource = source[0];
			batch.writeLongDelta(prevSource);
			batch.writeLongDelta(target[0]);

			for(int i = 1; i < n; i++) {
				if (source[i] != prevSource) {
					batch.writeLongDelta(source[i] - prevSource);
					batch.writeLongDelta(target[i]);
					prevSource = source[i];
				}
				else if (target[i] != target[i - 1]) {
					// We don't write duplicate pairs
					batch.writeDelta(0);
					batch.writeLongDelta(target[i] - target[i - 1] - 1);
				}
			}
		}
		else batch.writeDelta(0);

		batch.close();

		final File labelFile = File.createTempFile("label-", ".bits", tempDir);
		labelFile.deleteOnExit();
		labelBatches.add(labelFile);
		final OutputBitStream labelObs = new OutputBitStream(labelFile);
		for (int i = 0; i < n; i++) {
			labelBitStream.position(start[i]);
			prototype.fromBitStream(labelBitStream, source[i]);
			prototype.toBitStream(labelObs, target[i]);
		}
		labelObs.close();
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize) throws IOException {
		return transposeOffline(g, batchSize, null);
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ImmutableGraph, int, File, ProgressLogger)
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return transposeOffline(g, batchSize, tempDir, null);
	}

	/** Returns an immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * <p>This method creates a number of sorted batches on disk containing arcs
	 * represented by a pair of gap-compressed long integers ordered by target
	 * and returns an {@link ImmutableGraph}
	 * that can be accessed only using a {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator
	 * merges on the fly the batches, providing a transposed graph. The files are marked with
	 * {@link File#deleteOnExit()}, so they should disappear when the JVM exits. An additional safety-net
	 * finaliser tries to delete the batches, too.
	 *
	 * <p>Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the same time.
	 * The batches are closed when they are exhausted, so a complete scan of the graph closes them all. In any case,
	 * another safety-net finaliser closes all files when the iterator is collected.
	 *
	 * <P>This method can process {@linkplain ImmutableGraph#loadOffline(CharSequence) offline graphs}.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */

	public static ImmutableSequentialGraph transposeOffline(final ImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		long i, currNode;
		int j;
		final long[] source = new long[batchSize] , target = new long[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		final long n = g.numNodes();

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Creating sorted batches...");
		}

		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		LazyLongIterator succ;
		long m = 0; // Number of arcs, computed on the fly.
		j = 0;
		for(i = n; i-- != 0;) {
			currNode = nodeIterator.nextLong();
			final long d = nodeIterator.outdegree();
			succ = nodeIterator.successors();
			m += d;

			for(long k = 0; k < d; k++) {
				target[j] = currNode;
				source[j++] = succ.nextLong();

				if (j == batchSize) {
					processBatch(batchSize, source, target, tempDir, batches);
					j = 0;
				}
			}


			if (pl != null) pl.lightUpdate();
		}

		if (j != 0) processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			logBatches(batches, m, pl);
		}

		return new BatchGraph(n, m, batches);
	}

	protected static void logBatches(final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl) {
		long length = 0;
		for(File f : batches) length += f.length();
		pl.logger().info("Created " + batches.size() + " batches using " + Util.format((double)Byte.SIZE * length / pairs) + " bits/arc.");
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via a big array.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, long[][], int, File, ProgressLogger)
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final long map[][], final int batchSize) throws IOException {
		return mapOffline(g, map, batchSize, null);
	}

	/** Returns an immutable graph obtained by remapping offline the graph nodes through a partial function specified via a big array.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 * @see #mapOffline(ImmutableGraph, long[][], int, File, ProgressLogger)
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final long map[][], final int batchSize, final File tempDir) throws IOException {
		return mapOffline(g, map, batchSize, tempDir, null);
	}

	/** Remaps the the graph nodes through a partial function specified via
	 * a big array, using an offline method.
	 *
	 * <p>More specifically, <code>LongBigArrays.length(map)=g.numNodes()</code>,
	 * and <code>LongBigArrays.get(map, i)</code> is the new name of node <code>i</code>, or -1 if the node
	 * should not be mapped. If some
	 * index appearing in <code>map</code> is larger than or equal to the
	 * number of nodes of <code>g</code>, the resulting graph is enlarged correspondingly.
	 *
	 * <P>Arcs are mapped in the obvious way; in other words, there is
	 * an arc from <code>LongBigArrays.get(map, i)</code> to <code>LongBigArrays.get(map, j)</code> (both nonnegative)
	 * in the transformed
	 * graph iff there was an arc from <code>i</code> to <code>j</code>
	 * in the original graph.
	 *
	 *  <P>Note that if <code>map</code> is bijective, the returned graph
	 *  is simply a permutation of the original graph.
	 *  Otherwise, the returned graph is obtained by deleting nodes mapped
	 *  to -1, quotienting nodes w.r.t. the equivalence relation induced by the fibres of <code>map</code>
	 *  and renumbering the result, always according to <code>map</code>.
	 *
	 * See {@link #transposeOffline(ImmutableGraph, int, File, ProgressLogger)} for
	 * implementation and performance-related details.
	 *
	 * @param g an immutable graph.
	 * @param map the transformation map.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 * @return an immutable, sequentially accessible graph obtained by transforming <code>g</code>.
	 */
	public static ImmutableSequentialGraph mapOffline(final ImmutableGraph g, final long map[][], final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		int j;
		long d, mappedCurrNode;
		final long[] source = new long[batchSize] , target = new long[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		long max = -1;
		for(int i = map.length; i-- != 0;) {
			final long[] t = map[i];
			for(int k = t.length; k-- != 0;) max = Math.max(max, t[k]);
		}

		final long mapLength = LongBigArrays.length(map);

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = mapLength;
			pl.start("Creating sorted batches...");
		}

		final NodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <map[source],map[target]> (if we have to) and dumping them on disk.
		LazyLongIterator succ;
		j = 0;
		long pairs = 0; // Number of pairs
		for(long i = g.numNodes(); i-- != 0;) {
			mappedCurrNode = LongBigArrays.get(map, nodeIterator.nextLong());
			if (mappedCurrNode != -1) {
				d = nodeIterator.outdegree();
				succ = nodeIterator.successors();

				for(long k = 0; k < d; k++) {
					final long s = succ.nextLong();
					if (LongBigArrays.get(map, s) != -1) {
						source[j] = mappedCurrNode;
						target[j++] = LongBigArrays.get(map, s);

						if (j == batchSize) {
							pairs += processBatch(batchSize, source, target, tempDir, batches);
							j = 0;
						}
					}
				}
			}

			if (pl != null) pl.lightUpdate();
		}

		// At this point the number of nodes is always known (a traversal has been completed).
		if (g.numNodes() != mapLength) throw new IllegalArgumentException("Mismatch between number of nodes (" + g.numNodes() + ") and map length (" + mapLength + ")");

		if (j != 0) pairs += processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			logBatches(batches, pairs, pl);
		}

		return new BatchGraph(max + 1, -1, batches);
	}



	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */
	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize) throws IOException {
		return transposeOffline(g, batchSize, null);
	}

	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 * @see #transposeOffline(ArcLabelledImmutableGraph, int, File, ProgressLogger)
	 */
	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir) throws IOException {
		return transposeOffline(g, batchSize, tempDir, null);
	}


	/** Returns an arc-labelled immutable graph obtained by reversing all arcs in <code>g</code>, using an offline method.
	 *
	 * <p>This method creates a number of sorted batches on disk containing arcs
	 * represented by a pair of long integers in {@link java.io.DataInput} format ordered by target
	 * and returns an {@link ImmutableGraph}
	 * that can be accessed only using a {@link ImmutableGraph#nodeIterator() node iterator}. The node iterator
	 * merges on the fly the batches, providing a transposed graph. The files are marked with
	 * {@link File#deleteOnExit()}, so they should disappear when the JVM exits. An additional safety-net
	 * finaliser tries to delete the batches, too. As far as labels are concerned, they are temporarily stored in
	 * an in-memory bit stream, that is permuted when it is stored on the disk
	 *
	 * <p>Note that each {@link NodeIterator} returned by the transpose requires opening all batches at the same time.
	 * The batches are closed when they are exhausted, so a complete scan of the graph closes them all. In any case,
	 * another safety-net finaliser closes all files when the iterator is collected.
	 *
	 * <P>This method can process {@linkplain ArcLabelledImmutableGraph#loadOffline(CharSequence) offline graphs}. Note that
	 * no method to transpose on-line arc-labelled graph is provided currently.
	 *
	 * @param g an immutable graph.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method,
	 * plus an additional {@link FastByteArrayOutputStream} needed to store all the labels for a batch.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger.
	 * @return an immutable, sequentially accessible graph obtained by transposing <code>g</code>.
	 */

	public static ArcLabelledImmutableGraph transposeOffline(final ArcLabelledImmutableGraph g, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {

		int j;
		long d, currNode;
		final long[] source = new long[batchSize] , target = new long[batchSize];
		final long[] start = new long[batchSize];
		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		final ObjectArrayList<File> batches = new ObjectArrayList<>(), labelBatches = new ObjectArrayList<>();
		final Label prototype = g.prototype().copy();

		final long n = g.numNodes();

		if (pl != null) {
			pl.itemsName = "nodes";
			pl.expectedUpdates = n;
			pl.start("Creating sorted batches...");
		}

		final ArcLabelledNodeIterator nodeIterator = g.nodeIterator();

		// Phase one: we scan the graph, accumulating pairs <source,target> and dumping them on disk.
		LabelledArcIterator succ;
		Label[][] label = null;
		long m = 0; // Number of arcs, computed on the fly.
		j = 0;
		for(long i = n; i-- != 0;) {
			currNode = nodeIterator.nextLong();
			d = nodeIterator.outdegree();
			succ = nodeIterator.successors();
			label = nodeIterator.labelBigArray();
			m += d;

			for(long k = 0; k < d; k++) {
				source[j] = succ.nextLong();
				target[j] = currNode;
				start[j] = obs.writtenBits();
				ObjectBigArrays.get(label, k).toBitStream(obs, currNode);
				j++;

				if (j == batchSize) {
					obs.flush();
					processTransposeBatch(batchSize, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype);
					fbos = new FastByteArrayOutputStream();
					obs = new OutputBitStream(fbos); //ALERT here we should re-use
					j = 0;
				}
			}


			if (pl != null) pl.lightUpdate();
		}

		if (j != 0) {
			obs.flush();
			processTransposeBatch(j, source, target, start, new InputBitStream(fbos.array), tempDir, batches, labelBatches, prototype);
		}

		if (pl != null) {
			pl.done();
			logBatches(batches, m, pl);
		}

		final long numArcs = m;

		// Now we return an immutable graph whose nodeIterator() merges the batches on the fly.
		return new ArcLabelledImmutableSequentialGraph() {
			@Override
			public long numNodes() { return n; }
			@Override
			public long numArcs() { return numArcs; }

			@Override
			public ArcLabelledNodeIterator nodeIterator() {
				final long[] refArray = new long[batches.size()];
				final InputBitStream[] batchIbs = new InputBitStream[refArray.length];
				final InputBitStream[] labelInputBitStream = new InputBitStream[refArray.length];
				final int[] inputStreamLength = new int[refArray.length];
				final long[] prevTarget = new long[refArray.length];
				Arrays.fill(prevTarget, -1);
				// The indirect queue used to merge the batches.
				final LongHeapSemiIndirectPriorityQueue queue = new LongHeapSemiIndirectPriorityQueue(refArray);

				try {
					// We open all files and load the first element into the reference array.
					for(int i = 0; i < refArray.length; i++) {
						batchIbs[i] = new InputBitStream(batches.get(i));
						labelInputBitStream[i] = new InputBitStream(labelBatches.get(i));
						try {
							inputStreamLength[i] = batchIbs[i].readDelta();
							refArray[i] = batchIbs[i].readLongDelta();
						}
						catch (IOException e) {
							throw new RuntimeException(e);
						}

						queue.enqueue(i);
					}

					return new ArcLabelledNodeIterator() {
						/** The last returned node. */
						private long last = -1;
						/** The outdegree of the current node (valid if {@link #last} is not -1). */
						private long outdegree;
						/** The successors of the current node (valid if {@link #last} is not -1);
						 * only the first {@link #outdegree} entries are meaningful. */
						private long[][] successor = LongBigArrays.EMPTY_BIG_ARRAY;
						/** The labels of the arcs going out of the current node (valid if {@link #last} is not -1);
						 * only the first {@link #outdegree} entries are meaningful. */
						@SuppressWarnings("hiding")
						private Label[][] label = new Label[0][0];

						@Override
						public long outdegree() {
							if (last == -1) throw new IllegalStateException();
							return outdegree;
						}

						@Override
						public boolean hasNext() {
							return last < n - 1;
						}

						@Override
						public long nextLong() {
							last++;
							int d = 0;
							int i;

							try {
								/* We extract elements from the queue as long as their target is equal
								 * to last. If during the process we exhaust a batch, we close it. */

								while(! queue.isEmpty() && refArray[i = queue.first()] == last) {
									successor = LongBigArrays.grow(successor, d + 1);
									LongBigArrays.set(successor, d, prevTarget[i] += batchIbs[i].readLongDelta() + 1);
									label = ObjectBigArrays.grow(label, d + 1);
									final Label l = prototype.copy();
									ObjectBigArrays.set(label, d, l);
									l.fromBitStream(labelInputBitStream[i], last);

									if (--inputStreamLength[i] == 0) {
										queue.dequeue();
										batchIbs[i].close();
										labelInputBitStream[i].close();
										batchIbs[i] = null;
										labelInputBitStream[i] = null;
									}
									else {
										// We read a new source and update the queue.
										final long sourceDelta = batchIbs[i].readLongDelta();
										if (sourceDelta != 0) {
											refArray[i] += sourceDelta;
											prevTarget[i] = -1;
											queue.changed();
										}
									}
									d++;
								}
								// Neither quicksort nor heaps are stable, so we reestablish order here.
								it.unimi.dsi.fastutil.BigArrays.quickSort(0, d, new LongComparator() {
									@Override
									public int compare(long x, long y) {
										final long t = LongBigArrays.get(successor, x) - LongBigArrays.get(successor, y);
										return t == 0 ? 0 : t < 0 ? -1 : 1;
									}
								}, new BigSwapper() {
									@Override
									public void swap(long x, long y) {
										final long t = LongBigArrays.get(successor, x);
										LongBigArrays.set(successor, x, LongBigArrays.get(successor, y));
										LongBigArrays.set(successor, y, t);
										final Label l = ObjectBigArrays.get(label, x);
										ObjectBigArrays.set(label, x, ObjectBigArrays.get(label, y));
										ObjectBigArrays.set(label, y, l);
									}

								});
							}
							catch(IOException e) {
								throw new RuntimeException(e);
							}

							outdegree = d;
							return last;
						}

						@Override
						protected void finalize() throws Throwable {
							try {
								for(InputBitStream ibs: batchIbs) if (ibs != null) ibs.close();
								for(InputBitStream ibs: labelInputBitStream) if (ibs != null) ibs.close();
							}
							finally {
								super.finalize();
							}
						}

						@Override
						public LabelledArcIterator successors() {
							if (last == -1) throw new IllegalStateException();
							return new LabelledArcIterator() {
								@SuppressWarnings("hiding")
								int last = -1;

								@Override
								public Label label() {
									return ObjectBigArrays.get(label, last);
								}

								@Override
								public long nextLong() {
									if (last + 1 == outdegree) return -1;
									return LongBigArrays.get(successor, ++last);
								}

								@Override
								public long skip(long k) {
									long toSkip = Math.min(k, outdegree - last - 1);
									last += toSkip;
									return toSkip;
								}
							};
						}
					};
				}
				catch(IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			protected void finalize() throws Throwable {
				try {
					for(File f : batches) f.delete();
					for(File f : labelBatches) f.delete();
				}
				finally {
					super.finalize();
				}
			}
			@Override
			public Label prototype() {
				return prototype;
			}

		};
	}


	/** Returns the union of two arc-labelled immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param labelMergeStrategy the strategy used to merge labels when the same arc
	 *  is present in both graphs; if <code>null</code>, {@link Labels#KEEP_FIRST_MERGE_STRATEGY}
	 *  is used.
	 * @return the union of the two graphs.
	 */
	public static ArcLabelledImmutableGraph union(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelMergeStrategy labelMergeStrategy) {
		return new UnionArcLabelledImmutableGraph(g0, g1, labelMergeStrategy == null? Labels.KEEP_FIRST_MERGE_STRATEGY : labelMergeStrategy);
	}

	/** Returns the union of two immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the union of the two graphs.
	 */
	public static ImmutableGraph union(final ImmutableGraph g0, final ImmutableGraph g1) {
		return g0 instanceof ArcLabelledImmutableGraph && g1 instanceof ArcLabelledImmutableGraph
			? union((ArcLabelledImmutableGraph)g0, (ArcLabelledImmutableGraph)g1, (LabelMergeStrategy)null)
					: new UnionImmutableGraph(g0, g1);
	}


	private static final class ComposedGraph extends ImmutableSequentialGraph {
		private final ImmutableGraph g0;

		private final ImmutableGraph g1;

		private ComposedGraph(ImmutableGraph g0, ImmutableGraph g1) {
			this.g0 = g0;
			this.g1 = g1;
		}

		@Override
		public long numNodes() {
			return Math.max(g0.numNodes(), g1.numNodes());
		}

		@Override
		public ImmutableSequentialGraph copy() {
			// Note that only the second graph needs duplication.
			return new ComposedGraph(g0, g1.copy());
		}

		@Override
		public NodeIterator nodeIterator() {

			return new NodeIterator() {
				private final NodeIterator it0 = g0.nodeIterator();
				private long[][] succ = LongBigArrays.EMPTY_BIG_ARRAY;
				private final LongOpenHashSet successors = new LongOpenHashSet(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR);
				private int outdegree = -1; // -1 means that the cache is empty

				@Override
				public long nextLong() {
					outdegree = -1;
					return it0.nextLong();
				}

				@Override
				public boolean hasNext() {
					return it0.hasNext();
				}


				@Override
				public long outdegree() {
					if (outdegree < 0) successorBigArray();
					return outdegree;
				}

				@Override
				public long[][] successorBigArray() {
					if (outdegree < 0) {
						final int d = (int)it0.outdegree();
						final long[][] s = it0.successorBigArray();
						successors.clear();
						for (int i = 0; i < d; i++) {
							LazyLongIterator s1 = g1.successors(LongBigArrays.get(s, i));
							long x;
							while ((x = s1.nextLong()) >= 0) successors.add(x);
						}
						outdegree = successors.size();
						succ = LongBigArrays.ensureCapacity(succ, outdegree, 0);
						succ = LongBigArrays.newBigArray(outdegree);
						final LongIterator iterator = successors.iterator();
						for(long i = 0; i < outdegree; i++) LongBigArrays.set(succ, i, iterator.nextLong());
						LongBigArrays.quickSort(succ, 0, outdegree);
					}
					return succ;
				}
			};
		}
	}

	/** Returns the composition (a.k.a. matrix product) of two immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @return the composition of the two graphs.
	 */
	public static ImmutableGraph compose(final ImmutableGraph g0, final ImmutableGraph g1) {
		return new ComposedGraph(g0, g1);
	}


	/** Returns the composition (a.k.a. matrix product) of two arc-labelled immutable graphs.
	 *
	 * <P>The two arguments may differ in the number of nodes, in which case the
	 * resulting graph will be large as the larger graph.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 * @param strategy a label semiring.
	 * @return the composition of the two graphs.
	 */
	public static ArcLabelledImmutableGraph compose(final ArcLabelledImmutableGraph g0, final ArcLabelledImmutableGraph g1, final LabelSemiring strategy) {
		if (g0.prototype().getClass() != g1.prototype().getClass()) throw new IllegalArgumentException("The two graphs have different label classes (" + g0.prototype().getClass().getSimpleName() + ", " +g1.prototype().getClass().getSimpleName() + ")");

		return new ArcLabelledImmutableSequentialGraph() {

			@Override
			public Label prototype() {
				return g0.prototype();
			}

			@Override
			public long numNodes() {
				return Math.max(g0.numNodes(), g1.numNodes());
			}

			@Override
			public ArcLabelledNodeIterator nodeIterator() {

				return new ArcLabelledNodeIterator() {
					private final ArcLabelledNodeIterator it0 = g0.nodeIterator();
					private long[] succ = LongArrays.EMPTY_ARRAY;
					private Label[] label = new Label[0];
					private int maxOutDegree;
					private int smallCount;
					private Long2ObjectOpenHashMap<Label> successors = new Long2ObjectOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR);
					{
						successors.defaultReturnValue(strategy.zero());
					}
					private int outdegree = -1; // -1 means that the cache is empty

					@Override
					public long nextLong() {
						outdegree = -1;
						return it0.nextLong();
					}

					@Override
					public boolean hasNext() {
						return it0.hasNext();
					}


					@Override
					public long outdegree() {
						if (outdegree < 0) successorBigArray();
						return outdegree;
					}

					private void ensureCache() {
						if (outdegree < 0) {
							final long d = it0.outdegree();
							final LabelledArcIterator s = it0.successors();
							if (successors.size() < maxOutDegree / 2 && smallCount++ > 100) {
								smallCount = 0;
								maxOutDegree = 0;
								successors = new Long2ObjectOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, Hash.FAST_LOAD_FACTOR);
								successors.defaultReturnValue(strategy.zero());
							}
							else successors.clear();

							for (int i = 0; i < d; i++) {
								LabelledArcIterator s1 = g1.successors(s.nextLong());
								long x;
								while ((x = s1.nextLong()) >= 0) successors.put(x, strategy.add(strategy.multiply(s.label(), s1.label()), successors.get(x)));
							}
							outdegree = successors.size();
							succ = LongArrays.ensureCapacity(succ, outdegree, 0);
							label = ObjectArrays.ensureCapacity(label, outdegree, 0);
							successors.keySet().toArray(succ);
							LongArrays.quickSort(succ, 0, outdegree);
							for(int i = outdegree; i-- != 0;) label[i] = successors.get(succ[i]);
							if (outdegree > maxOutDegree) maxOutDegree = outdegree;
						}
					}

					@Override
					public long[][] successorBigArray() {
						ensureCache();
						return LongBigArrays.wrap(succ);
					}

					@Override
					public Label[][] labelBigArray() {
						ensureCache();
						return ObjectBigArrays.wrap(label);
					}

					@Override
					public LabelledArcIterator successors() {
						ensureCache();
						return new LabelledArcIterator() {
							int i = -1;
							@Override
							public Label label() {
								return label[i];
							}

							@Override
							public long nextLong() {
								return i < outdegree - 1 ? succ[++i] : -1;
							}

							@Override
							public long skip(final long n) {
								final int incr = (int)Math.min(n, outdegree - i - 1);
								i += incr;
								return incr;
							}
						};
					}
				};
			}
		};
	}


	/** Returns a permutation that would make the given graph adjacency lists in Gray-code order.
	 *
	 * <P>Gray codes list all sequences of <var>n</var> zeros and ones in such a way that
	 * adjacent lists differ by exactly one bit. If we assign to each row of the adjacency matrix of
	 * a graph its index as a Gray code, we obtain a permutation that will make similar lines
	 * nearer.
	 *
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the Gray-code permutation produced from a matrix that has been Gray-code sorted will
	 * <em>not</em> be, in general, the identity.
	 *
	 * <P>The important feature of Gray-code ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 *
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by Gray order
	 * (you can just pass it to {@link #mapOffline(ImmutableGraph, long[][], int, File, ProgressLogger)}).
	 */
	public static long[][] grayCodePermutation(final ImmutableGraph g) {
		final long n = g.numNodes();
		final long[][] perm = LongBigArrays.newBigArray(n);
		long i = n;
		while(i-- != 0) LongBigArrays.set(perm, i, i);

		final LongComparator grayComparator =  new LongComparator() {
			/* Remember that given a Gray code G (expressed as a 0-based sequence
			 * of n bits G[i]), the corresponding binary code B if defined as
			 * follows: B[n-1]=G[n-1], and B[i] = B[i+1] ^ G[i].
			 *
			 * Translating the formula above to our case (where we just have the increasing
			 * list of indices j such that G[i]=1), we see that the binary code
			 * corresponding to the Gray code of an adjacency list is
			 * made of alternating blocks of zeroes and ones; the alternation
			 * happens at each successor.
			 *
			 * Said that, the code below requires some reckoning to be fully
			 * understood (but it works!).
			 */

			@Override
			public int compare(final long x, final long y) {
				final LazyLongIterator i = g.successors(x), j = g.successors(y);
				long a;
				long b;

				/* This code duplicates eagerly of the behaviour of the lazy comparator
				   below. It is here for documentation and debugging purposes.

				byte[] g1 = new byte[g.numNodes()], g2 = new byte[g.numNodes()];
				while(i.hasNext()) g1[g.numNodes() - 1 - i.nextInt()] = 1;
				while(j.hasNext()) g2[g.numNodes() - 1 - j.nextInt()] = 1;
				for(int k = g.numNodes() - 2; k >= 0; k--) {
					g1[k] ^= g1[k + 1];
					g2[k] ^= g2[k + 1];
				}
				for(int k = g.numNodes() - 1; k >= 0; k--) if (g1[k] != g2[k]) return g1[k] - g2[k];
				return 0;
				*/

				boolean parity = false; // Keeps track of the parity of number of arcs before the current ones.
				for(;;) {
					a = i.nextLong();
					b = j.nextLong();
					if (a == -1 && b == -1) return 0;
					if (a == -1) return parity ? 1 : -1;
					if (b == -1) return parity ? -1 : 1;
					if (a != b) return parity ^ (a < b) ? 1 : -1;
					parity = ! parity;
				}
			}
		};

		LongBigArrays.quickSort(perm, 0, n, grayComparator);

		return Util.invertPermutationInPlace(perm);
	}

	/** Returns a random permutation for a given graph.
	 *
	 * @param g an immutable graph.
	 * @param seed for {@link XorShift1024StarRandom}.
	 * @return a random permutation for the given graph
	 */
	public static long[][] randomPermutation(final ImmutableGraph g, final long seed) {
		return LongBigArrays.shuffle(Util.identity(g.numNodes()), new XorShift1024StarRandom(seed));
	}



	/** Returns a permutation that would make the given graph adjacency lists in lexicographical order.
	 *
	 * <P>Note that since a graph permutation permutes <em>both</em> rows and columns, this transformation is
	 * not idempotent: the lexicographical permutation produced from a matrix that has been
	 * lexicographically sorted will
	 * <em>not</em> be, in general, the identity.
	 *
	 * <P>The important feature of lexicographical ordering is that it is completely endogenous (e.g., determined
	 * by the graph itself), contrarily to, say, lexicographic URL ordering (which relies on the knowledge
	 * of the URL associated to each node).
	 *
	 * <p><strong>Warning</strong>: rows are numbered from zero <em>from the left</em>. This means,
	 * for instance, that nodes with an arc towards node zero are lexicographically smaller
	 * than nodes without it.
	 *
	 * @param g an immutable graph.
	 * @return the permutation that would order the graph adjacency lists by lexicographical order
	 * (you can just pass it to {@link #mapOffline(ImmutableGraph, long[][], int)}).
	 */
	public static long[][] lexicographicalPermutation(final ImmutableGraph g) {
		final long n = g.numNodes();
		final long[][] perm = Util.identity(n);

		final LongComparator lexicographicalComparator =  new LongComparator() {
			@Override
			public int compare(final long x, final long y) {
				final LazyLongIterator i = g.successors(x), j = g.successors(y);
				long a;
				long b;
				for(;;) {
					a = i.nextLong();
					b = j.nextLong();
					if (a == -1 && b == -1) return 0;
					if (a == -1) return -1;
					if (b == -1) return 1;
					if (a != b) {
						final long t = b - a;
						return t == 0 ? 0 : t < 0 ? -1 : 1;
					}
				}
			}
		};

		LongBigArrays.quickSort(perm, 0, n, lexicographicalComparator);

		return Util.invertPermutationInPlace(perm);
	}



	/** Ensures that the arguments are exactly <code>n</code>, if <code>n</code> is nonnegative, or
	 * at least -<code>n</code>, otherwise.
	 */

	private static boolean ensureNumArgs(String param[], int n) {
		if (n >= 0 && param.length != n || n < 0 && param.length < -n) {
			System.err.println("Wrong number (" + param.length + ") of arguments.");
			return false;
		}
		return true;
	}

	/** Loads a graph with given data and returns it.
	 *
	 * @param graphClass the class of the graph to be loaded.
	 * @param baseName the graph basename.
	 * @param offline whether the graph is to be loaded in an offline fashion.
	 * @param pl a progress logger.
	 * @return the loaded graph.
	 */
	private static ImmutableGraph load(Class<?> graphClass, String baseName, boolean offline, ProgressLogger pl) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {
		ImmutableGraph graph = null;

		if (graphClass != null) {
			if (offline) graph = (ImmutableGraph)graphClass.getMethod("loadOffline", CharSequence.class).invoke(null, baseName);
			else graph = (ImmutableGraph)graphClass.getMethod("load", CharSequence.class, ProgressLogger.class).invoke(null, baseName, pl);
		}
		else graph = offline ?
			ImmutableGraph.loadOffline(baseName) :
			ImmutableGraph.load(baseName, pl);

		return graph;
	}



	public static void main(String args[]) throws IOException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, JSAPException {
		Class<?> sourceGraphClass = null, destGraphClass = BVGraph.class;
		boolean offline = true, ascii = false;

		final Field[] field = Transform.class.getDeclaredFields();
		final List<String> filterList = new ArrayList<>();
		final List<String> labelledFilterList = new ArrayList<>();

		for(Field f: field) {
			if (ArcFilter.class.isAssignableFrom(f.getType())) filterList.add(f.getName());
			if (LabelledArcFilter.class.isAssignableFrom(f.getType())) labelledFilterList.add(f.getName());
		}

		SimpleJSAP jsap = new SimpleJSAP(Transform.class.getName(),
				"Transforms one or more graphs. All transformations require, after the name,\n" +
				"some parameters specified below:\n" +
				"\n" +
				"identity                  sourceBasename destBasename\n" +
				"mapOffline                sourceBasename destBasename map [batchSize] [tempDir] [cutoff]\n" +
				"transposeOffline          sourceBasename destBasename [batchSize] [tempDir]\n" +
				"symmetrizeOffline         sourceBasename destBasename [batchSize] [tempDir]\n" +
				"union                     source1Basename source2Basename destBasename [strategy]\n" +
				"compose                   source1Basename source2Basename destBasename [semiring]\n" +
				"gray                      sourceBasename destBasename [batchSize] [tempDir]\n" +
				"grayPerm                  sourceBasename dest\n" +
				"lex                       sourceBasename destBasename [batchSize] [tempDir]\n" +
				"lexPerm                   sourceBasename dest\n" +
				"random                    sourceBasename destBasename [seed] [batchSize] [tempDir]\n" +
				"arcfilter                 sourceBasename destBasename arcFilter (available filters: " + filterList + ")\n" +
				"larcfilter                sourceBasename destBasename arcFilter (available filters: " + labelledFilterList + ")\n" +
				"\n" +
				"Please consult the Javadoc documentation for more information on each transform.",
				new Parameter[] {
						new FlaggedOption("sourceGraphClass", GraphClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "source-graph-class", "Forces a Java class to load the source graph."),
						new FlaggedOption("destGraphClass", GraphClassParser.getParser(), BVGraph.class.getName(), JSAP.NOT_REQUIRED, 'd', "dest-graph-class", "Forces a Java class to store the destination graph."),
						new FlaggedOption("destArcLabelledGraphClass", GraphClassParser.getParser(), BitStreamArcLabelledImmutableGraph.class.getName(), JSAP.NOT_REQUIRED, 'L', "dest-arc-labelled-graph-class", "Forces a Java class to store the labels of the destination graph."),
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new Switch("ascii", 'a', "ascii", "Maps are in ASCII form (one integer per line)."),
						new UnflaggedOption("transform", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The transformation to be applied."),
						new UnflaggedOption("param", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.GREEDY, "The remaining parameters."),
					}
				);

		JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		sourceGraphClass = jsapResult.getClass("sourceGraphClass");
		destGraphClass = jsapResult.getClass("destGraphClass");
		ascii = jsapResult.getBoolean("ascii");
		String transform = jsapResult.getString("transform");
		String[] param = jsapResult.getStringArray("param");

		String source[] = null, dest = null, map = null;
		ArcFilter arcFilter = null;
		LabelledArcFilter labelledArcFilter = null;
		LabelSemiring labelSemiring = null;
		LabelMergeStrategy labelMergeStrategy = null;
		int batchSize = 1000000;
		long cutoff = -1;
		long seed = 0;
		File tempDir = null;

		if (! ensureNumArgs(param, -2)) return;

		if (transform.equals("identity") || transform.equals("grayPerm") || transform.equals("lexPerm")) {
			source = new String[] { param[0] };
			dest = param[1];
			if (! ensureNumArgs(param, 2)) return;
		}
		else if (transform.equals("mapOffline")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0] };
			dest = param[1];
			map = param[2];
			if (param.length >= 4) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[3])).intValue();
				if (param.length >= 5) {
					tempDir = new File(param[4]);
					if (param.length == 6) cutoff = Long.parseLong(param[5]);
					else if (! ensureNumArgs(param, 5))	return;
				}
				else if (! ensureNumArgs(param, 4))	return;
			}
			else if (! ensureNumArgs(param, 3))	return;
		}
		else if (transform.equals("random")) {
			if (! ensureNumArgs(param, -2)) return;
			source = new String[] { param[0] };
			dest = param[1];
			if (param.length >= 3) {
				seed = Long.parseLong(param[2]);
				if (param.length >= 4) {
					batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[3])).intValue();
					if (param.length == 5) tempDir = new File(param[4]);
					else if (! ensureNumArgs(param, 4))	return;
				}
				else if (! ensureNumArgs(param, 3))	return;
			}
			else if (! ensureNumArgs(param, 2))	return;
		}
		else if (transform.equals("arcfilter")) {
			if (ensureNumArgs(param, 3)) {
				try {
					// First try: a public field
					arcFilter = (ArcFilter) Transform.class.getField(param[2]).get(null);
				}
				catch(NoSuchFieldException e) {
					// No chance: let's try with a class
					arcFilter = ObjectParser.fromSpec(param[2], ArcFilter.class, GraphClassParser.PACKAGE);
				}
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else return;
		}
		else if (transform.equals("larcfilter")) {
			if (ensureNumArgs(param, 3)) {
				try {
					// First try: a public field
					labelledArcFilter = (LabelledArcFilter) Transform.class.getField(param[2]).get(null);
				}
				catch(NoSuchFieldException e) {
					// No chance: let's try with a class
					labelledArcFilter = ObjectParser.fromSpec(param[2], LabelledArcFilter.class, GraphClassParser.PACKAGE);
				}
				source = new String[] { param[0], null };
				dest = param[1];
			}
			else return;
		}
		else if (transform.equals("union")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
			if (param.length == 4) labelMergeStrategy = ObjectParser.fromSpec(param[3], LabelMergeStrategy.class, GraphClassParser.PACKAGE);
			else if (! ensureNumArgs(param, 3)) return;
		}
		else if (transform.equals("compose")) {
			if (! ensureNumArgs(param, -3)) return;
			source = new String[] { param[0], param[1] };
			dest = param[2];
			if (param.length == 4) labelSemiring = ObjectParser.fromSpec(param[3], LabelSemiring.class, GraphClassParser.PACKAGE);
			else if (! ensureNumArgs(param, 3)) return;
		}
		else if (transform.equals("transposeOffline") || transform.equals("symmetrizeOffline") || transform.equals("removeDangling") || transform.equals("gray") || transform.equals("lex")) {
			if (! ensureNumArgs(param, -2)) return;
			source = new String[] { param[0] };
			dest = param[1];
			if (param.length >= 3) {
				batchSize = ((Integer)JSAP.INTSIZE_PARSER.parse(param[2])).intValue();
				if (param.length == 4) tempDir = new File(param[3]);
				else if (! ensureNumArgs(param, 3))	return;
			}
			else if (! ensureNumArgs(param, 2))	return;
		}
		else {
			System.err.println("Unknown transform: " + transform);
			return;
		}

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		final ImmutableGraph[] graph = new ImmutableGraph[source.length];
		final ImmutableGraph result;
		final Class<?> destLabelledGraphClass = jsapResult.getClass("destArcLabelledGraphClass");
		if (! ArcLabelledImmutableGraph.class.isAssignableFrom(destLabelledGraphClass)) throw new IllegalArgumentException("The arc-labelled destination class " + destLabelledGraphClass.getName() + " is not an instance of ArcLabelledImmutableGraph");

		// Check for transformations that require random access on the source graph.
		if (transform.equals("grayPerm") || transform.equals("lexPerm") || transform.equals("gray") || transform.equals("lex")) offline = false;

		for (int i = 0; i < source.length; i++)
			// Note that composition requires the second graph to be always random access.
			if (source[i] == null) graph[i] = null;
			else graph[i] = load(sourceGraphClass, source[i], offline && ! (i == 1 && transform.equals("compose")), pl);

		final boolean graph0IsLabelled = graph[0] instanceof ArcLabelledImmutableGraph;
		final ArcLabelledImmutableGraph graph0Labelled = graph0IsLabelled ? (ArcLabelledImmutableGraph)graph[0] : null;
		final boolean graph1IsLabelled = graph.length > 1 && graph[1] instanceof ArcLabelledImmutableGraph;

		String notForLabelled = "This transformation will just apply to the unlabelled graph; label information will be absent";

		if (transform.equals("identity")) result = graph[0];
		else if (transform.equals("mapOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			pl.start("Reading map...");

			final long n = graph[0].numNodes();
			final long[][] f = LongBigArrays.newBigArray(n);
			final long loaded;
			if (ascii) loaded = TextIO.loadLongs(map, f);
			else loaded = BinIO.loadLongs(map, f);

			if(n != loaded) throw new IllegalArgumentException("The source graph has " + n + " nodes, but the permutation contains " + loaded + " longs");

			// Delete from the graph all nodes whose index is above the cutoff, if any.
			if (cutoff != -1)
				for(int i = f.length; i-- != 0;) {
					final long[] t = f[i];
					for(int d = t.length; d-- != 0;) if (t[d] >= cutoff) t[d] = -1;
				}

			pl.count = n;
			pl.done();

			result = mapOffline(graph[0], f, batchSize, tempDir, pl);
			LOGGER.info("Transform computation completed.");
		}
		else if (transform.equals("arcfilter")) {
			if (graph0IsLabelled && ! (arcFilter instanceof LabelledArcFilter)) {
				LOGGER.warn(notForLabelled);
				result = filterArcs(graph[0], arcFilter, pl);
			}
			else result = filterArcs(graph[0], arcFilter, pl);
		}
		else if (transform.equals("larcfilter")) {
			if (! graph0IsLabelled) throw new IllegalArgumentException("Filtering on labelled arcs requires a labelled graph");
			result = filterArcs(graph0Labelled, labelledArcFilter, pl);
		}
		else if (transform.equals("symmetrizeOffline")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = symmetrizeOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("removeDangling")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);

			final long n = graph[0].numNodes();
			LOGGER.info("Finding dangling nodes...");

			final long[][] f = LongBigArrays.newBigArray(n);
			NodeIterator nodeIterator = graph[0].nodeIterator();
			int c = 0;
			for(long i = 0; i < n; i++) {
				nodeIterator.nextLong();
				LongBigArrays.set(f, i, nodeIterator.outdegree() != 0 ? c++ : -1);
			}
			result = mapOffline(graph[0], f, batchSize, tempDir, pl);
		}
		else if (transform.equals("transposeOffline")) {
			result = graph0IsLabelled ? transposeOffline(graph0Labelled, batchSize, tempDir, pl) : transposeOffline(graph[0], batchSize, tempDir, pl);
		}
		else if (transform.equals("union")) {
			if (graph0IsLabelled && graph1IsLabelled) {
				if (labelMergeStrategy == null) throw new IllegalArgumentException("Uniting labelled graphs requires a merge strategy");
				result = union(graph0Labelled,  (ArcLabelledImmutableGraph)graph[1], labelMergeStrategy);
			}
			else {
				if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
				result = union(graph[0], graph[1]);
			}
		}
		else if (transform.equals("compose")) {
			if (graph0IsLabelled && graph1IsLabelled) {
				if (labelSemiring == null) throw new IllegalArgumentException("Composing labelled graphs requires a composition strategy");
				result = compose(graph0Labelled, (ArcLabelledImmutableGraph)graph[1], labelSemiring);
			}
			else {
				if (graph0IsLabelled || graph1IsLabelled) LOGGER.warn(notForLabelled);
				result = compose(graph[0], graph[1]);
			}
		}
		else if (transform.equals("gray")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = mapOffline(graph[0], grayCodePermutation(graph[0]), batchSize, tempDir, pl);
		}
		else if (transform.equals("grayPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			BinIO.storeLongs(grayCodePermutation(graph[0]), param[1]);
			return;
		}
		else if (transform.equals("lex")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = mapOffline(graph[0], lexicographicalPermutation(graph[0]), batchSize, tempDir, pl);
		}
		else if (transform.equals("lexPerm")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			BinIO.storeLongs(lexicographicalPermutation(graph[0]), param[1]);
			return;
		}
		else if (transform.equals("random")) {
			if (graph0IsLabelled) LOGGER.warn(notForLabelled);
			result = mapOffline(graph[0], randomPermutation(graph[0], seed), batchSize, tempDir, pl);
		} else result = null;

		if (result instanceof ArcLabelledImmutableGraph) {
			// Note that we derelativise non-absolute pathnames to build the underlying graph name.
			LOGGER.info("The result is a labelled graph (class: " + destLabelledGraphClass.getName() + ")");
			final File destFile = new File(dest);
			final String underlyingName = (destFile.isAbsolute() ? dest : destFile.getName()) + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX;
			destLabelledGraphClass.getMethod("store", ArcLabelledImmutableGraph.class, CharSequence.class, CharSequence.class, ProgressLogger.class).invoke(null, result, dest, underlyingName, pl);
			ImmutableGraph.store(destGraphClass, result, dest + ArcLabelledImmutableGraph.UNDERLYINGGRAPH_SUFFIX, pl);
		}
		else ImmutableGraph.store(destGraphClass, result, dest, pl);
	}
}
