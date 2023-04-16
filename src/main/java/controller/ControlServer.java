package controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.FatalUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/*
Usage: curl http://localhost:9008/scale?stage=1
 */

class ControlServer {

    private CPServiceImpl svc;
    private int port;
    private Logger logger = LogManager.getLogger();

    public ControlServer(CPServiceImpl svc, int port) {
        // start the grpc server
        this.svc = svc;
        this.port = port;
    }

    public void start() throws IOException {
        HttpServer sv = HttpServer.create(new InetSocketAddress(port), 0);
        sv.createContext("/scale", new ScaleHandler(svc));
        sv.setExecutor(null);
        sv.start();
        logger.info("ControlPlane HTTP server started on port " + port);
    }

    public static Map<String, String> queryToMap(String query) {
        Map<String, String> result = new HashMap<>();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                try {
                    String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
                    String value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");
                    result.put(key, value);
                } catch (UnsupportedEncodingException e) {
                    // Handle exception as needed
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
}

class ScaleHandler implements HttpHandler {
    private CPServiceImpl svc;

    public ScaleHandler(CPServiceImpl svc) {
        this.svc = svc;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        Map<String, String> params = ControlServer.queryToMap(query);
        String stage = params.get("stage");
        if (stage != null) {
            // Process the 'stage' parameter as needed
            int stageVal = Integer.parseInt(stage);
            String response = "Received 'stage' parameter with value: " + stageVal;

            // TODO: call the CPServiceImpl to scale the pipeline

            exchange.sendResponseHeaders(200, response.length());
            OutputStream outputStream = exchange.getResponseBody();
            outputStream.write(response.getBytes());
            outputStream.close();
        } else {
            // 'stage' parameter not found, return error response
            String response = "Error: 'stage' parameter not found in query";
            exchange.sendResponseHeaders(400, response.length());
            OutputStream outputStream = exchange.getResponseBody();
            outputStream.write(response.getBytes());
            outputStream.close();
        }
    }
}