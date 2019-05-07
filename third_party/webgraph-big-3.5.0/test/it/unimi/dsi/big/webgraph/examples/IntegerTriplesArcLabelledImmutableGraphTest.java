package it.unimi.dsi.big.webgraph.examples;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.WebGraphTestCase;

import org.junit.Test;

public class IntegerTriplesArcLabelledImmutableGraphTest extends WebGraphTestCase {

	@Test
	public void testEmpty() {
		ImmutableGraph g = new IntegerTriplesArcLabelledImmutableGraph(new int[][] {});

		assertGraph(g);
	}

	@Test
	public void testCycle() {
		ImmutableGraph g = new IntegerTriplesArcLabelledImmutableGraph(new int[][] {
				{ 0, 1, 2 },
				{ 1, 2, 0 },
				{ 2, 0, 1 },

		});

		assertGraph(g);
	}

}
