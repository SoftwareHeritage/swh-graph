package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.Stack;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.Graph;

public class Visit
{
    public class Path extends ArrayList<String> {}

    Graph graphFwd;
    Graph graphBwd;

    private Graph graph;
    private LongArrayBitVector visited;
    private ArrayList<String> extraEdges;
    private Stack<Long> currentPath;
    private ArrayList<Path> paths;

    public Visit(Graph graph, Graph graphSym)
    {
        this.graphFwd = graph;
        this.graphBwd = graphSym;
    }

    public ArrayList<Path> visit(String start, ArrayList<String> extraEdges, boolean backward)
    {
        this.graph = (backward) ? this.graphBwd : this.graphFwd;
        this.visited = LongArrayBitVector.ofLength(graph.getNbNodes());
        this.extraEdges = extraEdges;
        this.paths = new ArrayList<Path>();
        this.currentPath = new Stack<Long>();

        _recursiveVisit(graph.getNode(start));

        return paths;
    }

    private void _recursiveVisit(long current)
    {
        visited.set(current);
        currentPath.push(current);

        long degree = graph.outdegree(current);
        if (degree == 0)
        {
            Path path = new Path();
            for (long node : currentPath)
                path.add(graph.getHash(node));
            paths.add(path);
        }

        LazyLongIterator successors = graph.successors(current);
        while (degree-- > 0)
        {
            long next = successors.nextLong();
            if (_traversalAllowed(current, next) && !visited.getBoolean(next))
                _recursiveVisit(next);
        }

        currentPath.pop();
    }

    private boolean _traversalAllowed(long current, long next)
    {
        // TODO
        return true;
    }
}
