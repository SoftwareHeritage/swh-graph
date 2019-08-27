package org.softwareheritage.graph.algo;

import java.util.function.BiConsumer;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.algo.NodeIdsConsumer;
import org.softwareheritage.graph.algo.Traversal;

/** Traversal algorithm implementations that stream results to the caller in
 * batch, to avoid loading in memory (potentially very) large results at once.
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.algo.Traversal
 */

public class StreamTraversal extends Traversal {

    static int DEFAULT_BUFFER_SIZE = 16_777_216;  // 128 MB / sizeof(long)

    /** number of node identifiers to buffer before returning to client */
    private int bufferSize;

    /** node identifier buffer */
    private long[] buffer;

    /** current position in the buffer */
    private int bufferPos;

    /** callback to be fired when buffer is full (or processing is done) */
    private NodeIdsConsumer cb;

    public StreamTraversal(Graph graph, String direction, String edgesFmt) {
	this(graph, direction, edgesFmt, DEFAULT_BUFFER_SIZE);
    }

    public StreamTraversal(Graph graph, String direction, String edgesFmt, int bufferSize) {
	super(graph, direction, edgesFmt);

	this.buffer = new long[bufferSize];
	this.bufferSize = bufferSize;
	this.bufferPos = 0;
    }

    private void bufferize(long nodeId) {
	buffer[bufferPos++] = nodeId;
	if (bufferPos == bufferSize) {
	    cb.accept(buffer, bufferPos);
	    bufferPos = 0;
	}
    }

    private void flush() {
	if (bufferPos > 0) {
	    cb.accept(buffer, bufferPos);
	    bufferPos = 0;
	}
    }

    public void leaves(long srcNodeId, NodeIdsConsumer cb) {
	this.cb = cb;
	this.leavesVisitor(srcNodeId, (nodeId) -> this.bufferize(nodeId));
	this.flush();
    }

    public void neighbors(long srcNodeId, NodeIdsConsumer cb) {
	this.cb = cb;
	this.neighborsVisitor(srcNodeId, (nodeId) -> this.bufferize(nodeId));
	this.flush();
    }

    public void visitNodes(long srcNodeId, NodeIdsConsumer cb) {
	this.cb = cb;
	this.visitNodesVisitor(srcNodeId, (nodeId) -> this.bufferize(nodeId));
	this.flush();
    }

}
