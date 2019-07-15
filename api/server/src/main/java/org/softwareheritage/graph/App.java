package org.softwareheritage.graph;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Stats;

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

    app.before("/stats/*", ctx -> { checkQueryStrings(ctx, ""); });
    app.before("/leaves/*", ctx -> { checkQueryStrings(ctx, "direction|edges"); });
    app.before("/neighbors/*", ctx -> { checkQueryStrings(ctx, "direction|edges"); });
    app.before("/visit/*", ctx -> { checkQueryStrings(ctx, "direction|edges"); });
    app.before("/walk/*", ctx -> { checkQueryStrings(ctx, "direction|edges|traversal"); });

    app.get("/stats/", ctx -> { ctx.json(stats); });

    // Graph traversal endpoints
    // By default the traversal is a forward DFS using all edges

    app.get("/leaves/:src", ctx -> {
      SwhId src = new SwhId(ctx.pathParam("src"));
      String direction = ctx.queryParam("direction", "forward");
      String edgesFmt = ctx.queryParam("edges", "*");

      Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
      ctx.json(endpoint.leaves(src));
    });

    app.get("/neighbors/:src", ctx -> {
      SwhId src = new SwhId(ctx.pathParam("src"));
      String direction = ctx.queryParam("direction", "forward");
      String edgesFmt = ctx.queryParam("edges", "*");

      Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
      ctx.json(endpoint.neighbors(src));
    });

    // TODO: anonymous class to return both nodes/paths? (waiting on node types map merged/refactor)
    /*app.get("/visit/:src", ctx -> {
      SwhId src = new SwhId(ctx.pathParam("src"));
      String direction = ctx.queryParam("direction", "forward");
      String edgesFmt = ctx.queryParam("edges", "*");

      Visit visit = new Visit(graph, src, edgesFmt, direction, Visit.OutputFmt.NODES_AND_PATHS);
      ctx.json(visit);
    });*/

    app.get("/visit/nodes/:src", ctx -> {
      SwhId src = new SwhId(ctx.pathParam("src"));
      String direction = ctx.queryParam("direction", "forward");
      String edgesFmt = ctx.queryParam("edges", "*");

      Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
      ctx.json(endpoint.visitNodes(src));
    });

    app.get("/visit/paths/:src", ctx -> {
      SwhId src = new SwhId(ctx.pathParam("src"));
      String direction = ctx.queryParam("direction", "forward");
      String edgesFmt = ctx.queryParam("edges", "*");

      Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
      ctx.json(endpoint.visitPaths(src));
    });

    app.get("/walk/:src/:dst", ctx -> {
      SwhId src = new SwhId(ctx.pathParam("src"));
      String dstFmt = ctx.pathParam("dst");
      String direction = ctx.queryParam("direction", "forward");
      String edgesFmt = ctx.queryParam("edges", "*");
      String algorithm = ctx.queryParam("traversal", "dfs");

      Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
      ctx.json(endpoint.walk(src, dstFmt, algorithm));
    });

    app.exception(IllegalArgumentException.class, (e, ctx) -> {
      ctx.status(400);
      ctx.result(e.getMessage());
    });
  }

  private static void checkQueryStrings(Context ctx, String allowedFmt) {
    Map<String, List<String>> queryParamMap = ctx.queryParamMap();
    for (String key : queryParamMap.keySet()) {
      if (!key.matches(allowedFmt)) {
        throw new IllegalArgumentException("Unknown query string: " + key);
      }
    }
  }
}
