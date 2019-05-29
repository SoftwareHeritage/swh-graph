package org.softwareheritage.graph;

import java.io.IOException;
import java.util.Optional;

import io.javalin.Javalin;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Stats;
import org.softwareheritage.graph.algo.Visit;

public class App {
  public static void main(String[] args) throws IOException, Exception {
    String path = args[0];
    Graph graph = new Graph(path);

    Javalin app = Javalin.create().start(5010);

    app.get("/stats/:src_type/:dst_type", ctx -> {
      try {
        String srcType = ctx.pathParam("src_type");
        String dstType = ctx.pathParam("dst_type");
        ctx.json(new Stats(srcType, dstType));
      } catch (IllegalArgumentException e) {
        ctx.status(404);
      } catch (Exception e) {
        ctx.status(400);
        ctx.result(e.toString());
      }
    });

    app.get("/visit/:swh_id", ctx -> {
      try {
        SwhId start = new SwhId(ctx.pathParam("swh_id"));

        // By default, traversal is a forward DFS using all edges
        String algorithm = Optional.ofNullable(ctx.queryParam("traversal")).orElse("dfs");
        String direction = Optional.ofNullable(ctx.queryParam("direction")).orElse("forward");
        String edges = Optional.ofNullable(ctx.queryParam("edges")).orElse("cnt:dir:rel:rev:snp");

        // TODO: Use transposed graph depending on 'direction'
        ctx.json(new Visit(graph, start, edges, algorithm));
      } catch (IllegalArgumentException e) {
        ctx.status(400);
        ctx.result(e.toString());
      }
    });

    app.error(404, ctx -> { ctx.result("Not found"); });
  }
}
