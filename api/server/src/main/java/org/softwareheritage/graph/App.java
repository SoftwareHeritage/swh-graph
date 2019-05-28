package org.softwareheritage.graph;

import java.io.IOException;
import java.util.Optional;

import io.javalin.Javalin;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.algo.Stats;
import org.softwareheritage.graph.algo.Visit;

public class App {
  public static void main(String[] args) throws IOException, Exception {
    String path = args[0];
    Graph graph = new Graph(path);
    Stats stats = new Stats(graph);

    Javalin app = Javalin.create().start(5010);

    app.get("/stats/", ctx -> {
      try {
        ctx.json(stats);
      } catch (IllegalArgumentException e) {
        ctx.status(404);
      } catch (Exception e) {
        ctx.status(400);
        ctx.result(e.toString());
      }
    });

    app.get("/visit/:swh_id", ctx -> {
      String start = ctx.pathParam("swh_id");

      // By default, traversal is a forward DFS using all edges
      String algorithm = Optional.ofNullable(ctx.queryParam("traversal")).orElse("dfs");
      String direction = Optional.ofNullable(ctx.queryParam("direction")).orElse("forward");
      String edges = Optional.ofNullable(ctx.queryParam("edges")).orElse("cnt:dir:rel:rev:snp");

      // TODO: Use transposed graph depending on 'direction'
      ctx.json(new Visit(graph, start, edges, algorithm));
    });

    app.error(404, ctx -> { ctx.result("Not found"); });
  }
}
