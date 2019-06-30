package org.softwareheritage.graph;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import io.javalin.Javalin;
import io.javalin.plugin.json.JavalinJackson;

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

    // Configure Jackson JSON to use snake case naming style
    ObjectMapper objectMapper = JavalinJackson.getObjectMapper();
    objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    JavalinJackson.configure(objectMapper);

    Javalin app = Javalin.create().start(5009);

    app.get("/stats", ctx -> {
      ctx.json(stats);
    });

    app.get("/visit/:swh_id", ctx -> {
      try {
        Map<String, List<String>> queryParamMap = ctx.queryParamMap();
        for (String key : queryParamMap.keySet()) {
          if (!key.matches("direction|edges|traversal")) {
            throw new IllegalArgumentException("Unknown query string: " + key);
          }
        }

        SwhId swhId = new SwhId(ctx.pathParam("swh_id"));

        // By default, traversal is a forward DFS using all edges
        String traversal = ctx.queryParam("traversal", "dfs");
        String direction = ctx.queryParam("direction", "forward");
        String edges = ctx.queryParam("edges", "all");

        ctx.json(new Visit(graph, swhId, edges, traversal, direction));
      } catch (IllegalArgumentException e) {
        ctx.status(400);
        ctx.result(e.getMessage());
      }
    });

    app.error(404, ctx -> { ctx.result("Not found"); });
  }
}
