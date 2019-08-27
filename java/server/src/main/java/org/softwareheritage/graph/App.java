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
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.algo.Stats;

/**
 * Web framework of the swh-graph server REST API.
 *
 * @author The Software Heritage developers
 */

public class App {
    /**
     * Main entrypoint.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException, JSAPException {
        SimpleJSAP jsap = new SimpleJSAP(App.class.getName(),
                                         "Server to load and query a compressed graph representation of Software Heritage archive.",
                                         new Parameter[] {
                                             new FlaggedOption("port", JSAP.INTEGER_PARSER, "5009", JSAP.NOT_REQUIRED, 'p', "port",
                                                     "Binding port of the server."),
                                             new UnflaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                                     JSAP.NOT_GREEDY, "The basename of the compressed graph."),
                                             new Switch("timings", 't', "timings", "Show timings in API result metadata."),
                                         });

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }

        String graphPath = config.getString("graphPath");
        int port = config.getInt("port");
        boolean showTimings = config.getBoolean("timings");

        startServer(graphPath, port, showTimings);
    }

    /**
     * Loads compressed graph and starts the web server to query it.
     *
     * @param graphPath basename of the compressed graph
     * @param port binding port of the server
     * @param showTimings true if timings should be in results metadata, false otherwise
     */
    private static void startServer(String graphPath, int port, boolean showTimings)
    throws IOException {
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
            SwhPID src = new SwhPID(ctx.pathParam("src"));
            String direction = ctx.queryParam("direction", "forward");
            String edgesFmt = ctx.queryParam("edges", "*");

            Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
            Endpoint.Output output = endpoint.leaves(new Endpoint.Input(src));
            ctx.json(formatEndpointOutput(output, showTimings));
        });

        app.get("/neighbors/:src", ctx -> {
            SwhPID src = new SwhPID(ctx.pathParam("src"));
            String direction = ctx.queryParam("direction", "forward");
            String edgesFmt = ctx.queryParam("edges", "*");

            Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
            Endpoint.Output output = endpoint.neighbors(new Endpoint.Input(src));
            ctx.json(formatEndpointOutput(output, showTimings));
        });

        app.get("/visit/nodes/:src", ctx -> {
            SwhPID src = new SwhPID(ctx.pathParam("src"));
            String direction = ctx.queryParam("direction", "forward");
            String edgesFmt = ctx.queryParam("edges", "*");

            Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
            Endpoint.Output output = endpoint.visitNodes(new Endpoint.Input(src));
            ctx.json(formatEndpointOutput(output, showTimings));
        });

        app.get("/visit/paths/:src", ctx -> {
            SwhPID src = new SwhPID(ctx.pathParam("src"));
            String direction = ctx.queryParam("direction", "forward");
            String edgesFmt = ctx.queryParam("edges", "*");

            Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
            Endpoint.Output output = endpoint.visitPaths(new Endpoint.Input(src));
            ctx.json(formatEndpointOutput(output, showTimings));
        });

        app.get("/walk/:src/:dst", ctx -> {
            SwhPID src = new SwhPID(ctx.pathParam("src"));
            String dstFmt = ctx.pathParam("dst");
            String direction = ctx.queryParam("direction", "forward");
            String edgesFmt = ctx.queryParam("edges", "*");
            String algorithm = ctx.queryParam("traversal", "dfs");

            Endpoint endpoint = new Endpoint(graph, direction, edgesFmt);
            Endpoint.Output output = endpoint.walk(new Endpoint.Input(src, dstFmt, algorithm));
            ctx.json(formatEndpointOutput(output, showTimings));
        });

        app.exception(IllegalArgumentException.class, (e, ctx) -> {
            ctx.status(400);
            ctx.result(e.getMessage());
        });
    }

    /**
     * Checks query strings names provided to the REST API.
     *
     * @param ctx Javalin HTTP request context
     * @param allowedFmt a regular expression describing allowed query strings names
     * @throws IllegalArgumentException unknown query string provided
     */
    private static void checkQueryStrings(Context ctx, String allowedFmt) {
        Map<String, List<String>> queryParamMap = ctx.queryParamMap();
        for (String key : queryParamMap.keySet()) {
            if (!key.matches(allowedFmt)) {
                throw new IllegalArgumentException("Unknown query string: " + key);
            }
        }
    }

    /**
     * Formats endpoint result into final JSON for the REST API.
     * <p>
     * Removes unwanted information if necessary, such as timings (to prevent use of side channels
     * attacks).
     *
     * @param output endpoint operation output which needs formatting
     * @param showTimings true if timings should be in results metadata, false otherwise
     * @return final Object with desired JSON format
     */
    private static Object formatEndpointOutput(Endpoint.Output output, boolean showTimings) {
        if (showTimings) {
            return output;
        } else {
            Map<String, Object> metaNoTimings = Map.of("nb_edges_accessed", output.meta.nbEdgesAccessed);
            Map<String, Object> outputNoTimings = Map.of("result", output.result, "meta", metaNoTimings);
            return outputNoTimings;
        }
    }
}
