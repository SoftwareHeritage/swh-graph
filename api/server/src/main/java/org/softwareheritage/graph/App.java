package org.softwareheritage.graph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import io.javalin.Javalin;

import org.softwareheritage.graph.Dataset;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.algo.Stats;

public class App
{
    public static void main(String[] args)
    {
        Path path = Paths.get(args[0]);
        Graph graph = new Graph(path.toString());

        Stats stats = new Stats(graph);

        Javalin app = Javalin.create().start(5010);

        app.get("/stats/:dataset", ctx -> {
            try {
                String dataset = ctx.pathParam("dataset").toUpperCase();
                ctx.json(stats.getStats(Dataset.Name.valueOf(dataset)));
            } catch (IllegalArgumentException | IOException e) {
                ctx.status(404);
            } catch (Exception e) {
                ctx.status(400);
                ctx.result(e.toString());
            }
        });

        app.error(404, ctx -> {
            ctx.result("Not found");
        });
    }
}
