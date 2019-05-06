package org.softwareheritage.graph;

import io.javalin.Javalin;

import org.softwareheritage.graph.GraphAPI;

public class App
{
    public static void main(String[] args)
    {
        GraphAPI graph = new GraphAPI();

        Javalin app = Javalin.create().start(5010);
        app.get("/nb_nodes", ctx -> {
            ctx.json(graph.nbNodes());
        });
    }
}
