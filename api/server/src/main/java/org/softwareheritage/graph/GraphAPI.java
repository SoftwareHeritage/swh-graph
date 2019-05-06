package org.softwareheritage.graph;

import java.io.IOException;

import it.unimi.dsi.webgraph.BVGraph;

public class GraphAPI
{
    String graphName;
    BVGraph graph;

    public GraphAPI(String graphName)
    {
        this.graphName = graphName;
        try {
            this.graph = BVGraph.load(graphName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int nbNodes()
    {
        return graph.numNodes();
    }
}
