package org.softwareheritage.graph;

import java.util.EnumMap;

import org.softwareheritage.graph.Dataset;

public class Graph
{
    EnumMap<Dataset.Name, Dataset> graph;
    String path;

    public Graph(String graphPath)
    {
        this.graph = new EnumMap<Dataset.Name, Dataset>(Dataset.Name.class);
        this.path = graphPath;
        if (!path.endsWith("/"))
            path += "/";

        for (Dataset.Name dataset : Dataset.Name.values())
            addDataset(dataset);
    }

    public void addDataset(Dataset.Name dataset)
    {
        String datasetPath = path + dataset.name().toLowerCase();
        graph.put(dataset, new Dataset(datasetPath));
    }

    public Dataset getDataset(Dataset.Name dataset)
    {
        return graph.get(dataset);
    }
}
