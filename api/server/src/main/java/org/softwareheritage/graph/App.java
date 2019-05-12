package org.softwareheritage.graph;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.javalin.Javalin;

import org.softwareheritage.graph.Dataset;
import org.softwareheritage.graph.Graph;

public class App
{
    public static void main(String[] args)
    {
        Path path = Paths.get(args[0]);
        Graph graph = new Graph(path.toString());

        Javalin app = Javalin.create().start(5010);
        app.get("/nb_nodes", ctx -> {
            ctx.json(graph.nbNodes());
        });
    }
}
