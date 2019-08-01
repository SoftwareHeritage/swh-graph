package org.softwareheritage.graph;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Stats;

/**
 * Entrypoint of the swh-graph server REST API.
 *
 * @author Thibault Allançon
 * @version 1.0
 * @since 1.0
 */

public class App {
  public static void main(String[] args) throws IOException, JSAPException {
    SimpleJSAP jsap = new SimpleJSAP(
        App.class.getName(),
        "Server to load and query a compressed graph representation of Software Heritage archive.",
        new Parameter[] {
          new FlaggedOption("port", JSAP.INTEGER_PARSER, "5009", JSAP.NOT_REQUIRED, 'p', "port",
              "Binding port of the server."),
          new UnflaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
              JSAP.NOT_GREEDY, "The basename of the compressed graph."),
        }
    );

    JSAPResult config = jsap.parse(args);
    if (jsap.messagePrinted()) {
      System.exit(1);
    }

    String graphPath = config.getString("graphPath");
    int port = config.getInt("port");

    startServer(graphPath, port);
  }

  private static void startServer(String graphPath, int port) throws IOException {
    Graph graph = new Graph(graphPath);
    Stats stats = new Stats(graphPath);

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

    Javalin app = Javalin.create().start(port);

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
