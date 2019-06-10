package org.softwareheritage.graph;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Stats;
import org.softwareheritage.graph.algo.Visit;

public class App {
  public static void main(String[] args) throws IOException {
    String path = args[0];
    Graph graph = new Graph(path);
    Stats stats = new Stats(path);

    // Clean up on exit
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          graph.cleanUp();
        } catch (IOException e) {
          System.out.println("Could not clean up graph on exit: " + e);
        }
      }
    });

    // Configure Jackson JSON properties
    ObjectMapper objectMapper = JavalinJackson.getObjectMapper();
    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    JavalinJackson.configure(objectMapper);

    Javalin app = Javalin.create().start(5010);

    app.get("/stats", ctx -> {
      ctx.json(stats);
    });

    app.get("/visit/:swh_id", ctx -> {
      try {
        SwhId start = new SwhId(ctx.pathParam("swh_id"));

        // By default, traversal is a forward DFS using all edges
        String algorithm = Optional.ofNullable(ctx.queryParam("traversal")).orElse("dfs");
        String direction = Optional.ofNullable(ctx.queryParam("direction")).orElse("forward");
        String edges = Optional.ofNullable(ctx.queryParam("edges")).orElse("all");

        ctx.json(new Visit(graph, start, edges, algorithm, direction));
      } catch (IllegalArgumentException e) {
        ctx.status(400);
        ctx.result(e.getMessage());
      }
    });

    app.error(404, ctx -> { ctx.result("Not found"); });
  }
}
