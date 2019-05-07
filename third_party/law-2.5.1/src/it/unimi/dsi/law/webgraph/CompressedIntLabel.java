package it.unimi.dsi.law.webgraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;

/*
 * Copyright (C) 2007-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.compression.Coder;
import it.unimi.dsi.compression.Decoder;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.labelling.AbstractIntLabel;

//RELEASE-STATUS: DIST

/** An integer label that uses a coder/decoder pair depending on the source node.
 *
 * <p>This is a kind of int label whose serialization ({@link #fromBitStream(InputBitStream, int)}
 * and {@link #toBitStream(OutputBitStream, int)} methods) rely on a coder/decoder pair that may depend on the source node of the arc.
 * Different constructors provide different ways to assign coders/decoders to source nodes: consult their documentation for more information.
 *
 * <p>More precisely, the public field {@link #nodeLabels} exposes a list of labels for nodes. Decoders are chosen depending on the label provided
 * by the list.
 */
public class CompressedIntLabel extends AbstractIntLabel implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final boolean DEBUG = false;
	/** The node labels stream containing the node labels. */
	public final LongBigList nodeLabels;
	/** A map assigning the coder to be used for every given source. */
	private final Int2ObjectMap<Coder> source2Coder;
	/** A map assigning the decoder to be used for every given source. */
	private final Int2ObjectMap<Decoder> source2Decoder;
	/** If not {@code null}, this label was produced with a specific spec, that is contained here. */
	private String labelSpec;

	protected CompressedIntLabel(final String key, final int value, final String labelSpec, final LongBigList nodeLabels, final Int2ObjectMap<Coder> source2Coder, final Int2ObjectMap<Decoder> source2Decoder) {
		super(key, value);
		this.labelSpec = labelSpec;
		this.nodeLabels = nodeLabels;
		this.source2Coder = source2Coder;
		this.source2Decoder = source2Decoder;
	}

	/** Creates a compressed integer label.
	 *
	 * @param key the key.
	 * @param value the value.
	 * @param nodeLabels the node labels.
	 * @param sourceLabel2Decoder a map assigning a decoder to every possible node label; note that the number of node labels is 2<sup><var>w</var></sup>, where <var>w</var> is <code>nodeLabelPrototype.fixedWidth()</code>.
	 * @param sourceLabel2Coder an optional map assigning a coder to every possible node label, or {@code null}; note that the number of node labels is 2<sup><var>w</var></sup>, where <var>w</var> is <code>nodeLabelPrototype.fixedWidth()</code>.
	 */
	public CompressedIntLabel(final String key, final int value, final LongBigList nodeLabels, final Int2ObjectMap<Decoder> sourceLabel2Decoder, final Int2ObjectMap<Coder> sourceLabel2Coder) {
		super(key, value);
		this.nodeLabels = nodeLabels;
		this.source2Coder = sourceLabel2Coder;
		this.source2Decoder = sourceLabel2Decoder;
	}

	private static LongBigList loadLabels(final File nodeLabels, final int nodeWidth) throws IOException {
		final InputBitStream ibs = new InputBitStream(nodeLabels);
		final int n = (int)((nodeLabels.length() * Byte.SIZE) / nodeWidth);
		final LongBigList l = LongArrayBitVector.getInstance(n * nodeWidth).asLongBigList(nodeWidth);
		for(int i = 0; i < n; i++) l.add(ibs.readInt(nodeWidth));
		ibs.close();
		return l;
	}

	/** Creates a compressed integer label from a specification that includes decoders and coders.
	 *
	 *  <p><strong>Warning</strong>: the entire node-label stream is loaded into memory by this method.
	 *
	 * @param key the key of this label.
	 * @param value the value of this label.
	 * @param labels the filename of the file of labels.
	 * @param nodeWidth the width in bits of a node label.
	 * @param decoders the filename of a serialised {@link Int2ObjectMap} mapping node labels to decoders.
	 * @param coders the filename of a serialised {@link Int2ObjectMap} mapping node labels to coders.
	 */
	@SuppressWarnings("unchecked")
	public CompressedIntLabel(final Object directory, final String key, final String value, final String labels, final String nodeWidth, final String decoders, final String coders) throws NumberFormatException, FileNotFoundException, IOException, ClassNotFoundException {
		this(key, Integer.parseInt(value), loadLabels(new File((File)directory, labels), Integer.parseInt(nodeWidth)),
				(Int2ObjectMap<Decoder>)BinIO.loadObject(new File((File)directory, decoders)), (Int2ObjectMap<Coder>)BinIO.loadObject(new File((File)directory, coders)));
		this.labelSpec = key + "," + value + "," + labels + "," + nodeWidth + "," + decoders +"," + coders;
	}

	/** Creates a compressed integer label from a specification that includes just decoders.
	 *
	 *  <p><strong>Warning</strong>: the entire node-label stream is loaded into memory by this method.
	 *
	 * @param key the key of this label.
	 * @param value the value of this label.
	 * @param labels the filename of the file of nodes labels.
	 * @param nodeWidth the width in bits of a node label.
	 * @param decoders the filename of a serialised {@link Int2ObjectMap} mapping node labels to decoders.
	 */
	@SuppressWarnings("unchecked")
	public CompressedIntLabel(final Object directory, final String key, final String value, final String labels, final String nodeWidth, final String decoders) throws NumberFormatException, FileNotFoundException, IOException, ClassNotFoundException {
		this(key, Integer.parseInt(value), loadLabels(new File((File)directory, labels), Integer.parseInt(nodeWidth)),
				(Int2ObjectMap<Decoder>)BinIO.loadObject(new File((File)directory, decoders)), null);
		this.labelSpec = key + "," + value + "," + labels + "," + nodeWidth + "," + decoders;
	}

	@Override
	public CompressedIntLabel copy() {
		return new CompressedIntLabel(key, value, labelSpec, nodeLabels, source2Coder, source2Decoder);
	}

	@Override
	public int fromBitStream(final InputBitStream inputBitStream, final int source) throws IOException {
		return value = source2Decoder.get((int)nodeLabels.getLong(source)).decode(inputBitStream);
	}

	@Override
	public int toBitStream(final OutputBitStream outputBitStream, final int source) throws IOException {
		if (source2Coder == null) throw new UnsupportedOperationException();
		return source2Coder.get((int)nodeLabels.getLong(source)).encode(value, outputBitStream);
	}

	@Override
	public int fixedWidth() {
		return -1;
	}

	@Override
	public String toSpec() {
		return this.getClass().getName() + "(" + (labelSpec == null? key : labelSpec) + ")";
	}
}
