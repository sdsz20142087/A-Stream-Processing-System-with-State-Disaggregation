package controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import operators.BaseOperator;
import operators.stateful.ServerCountOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.A;
import pb.Tm;
import utils.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.*;

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
    private Logger logger = LogManager.getLogger();

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

            String resp = tryScale(stageVal);

            exchange.sendResponseHeaders(200, response.length());
            OutputStream outputStream = exchange.getResponseBody();
            outputStream.write(response.getBytes());
            outputStream.write(resp.getBytes());
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

    private String tryScale(int stage) {
        if(stage != 3){
            logger.error("Only stage 3 can be scaled");
            return "\nOnly stage 3 can be scaled";
        }
        try {
            // find the right TM with the source
            TMClient sourceTMClient = svc.tmClients.get(NodeBase.getHost()+":8018");
            TMClient newOpTMClient = svc.tmClients.get(NodeBase.getHost()+":8028");
            logger.info("tmClients:{}", svc.tmClients);
            assert sourceTMClient != null;
            assert newOpTMClient != null;
            BaseOperator newOp = new ServerCountOperator(new WikiInfoSerde(), new StringSerde(), 1, 12);
            newOp.setKeySelector(new WikiKeySelector());
            // config for new operator
            Tm.OperatorConfig.Builder newOpCfgBuilder = Tm.OperatorConfig.newBuilder()
                    .setNoOverride(true)
                    .setLogicalStage(3)
                    .setName("SvCountOperator_1-1")
                    .setPartitionStrategy(Tm.PartitionStrategy.ROUND_ROBIN)
                    .setPartitionPlan(
                            Tm.PartitionPlan.newBuilder()
                                    .setPartitionStart(Integer.MIN_VALUE)
                                    .setPartitionEnd(-1)
                                    .build())
                    .addAllOutputMetadata(List.of(new Tm.OutputMetadata[]{
                            Tm.OutputMetadata.newBuilder()
                                    .setName("SinkOperator_1-0")
                                    .setAddress(NodeBase.getHost()+":8018").build()
                    }));
            newOpTMClient.addOperator(newOpCfgBuilder.build(), newOp);

            // config for old operator in this stage
            Tm.OperatorConfig.Builder oldOpCfgBuilder = Tm.OperatorConfig.newBuilder()
                    .setLogicalStage(4)
                    .setName("SvCountOperator_1-0")
                    .setPartitionStrategy(Tm.PartitionStrategy.ROUND_ROBIN)
                    .addAllOutputMetadata(List.of(new Tm.OutputMetadata[]{
                                    Tm.OutputMetadata.newBuilder()
                                            .setName("SinkOperator_1-0")
                                            .setAddress(NodeBase.getHost()+":8018")
                                            .build()
                            })
                    ).setPartitionPlan(
                            Tm.PartitionPlan.newBuilder()
                                    .setPartitionStart(0)
                                    .setPartitionEnd(Integer.MAX_VALUE)
                                    .build()
                    );

            // new config for previous stage
            Tm.OperatorConfig.Builder prevStageOpCfg = Tm.OperatorConfig.newBuilder()
                    .setLogicalStage(3)
                    .setName("StatefulCPUHeavyOperator_1-0")
                    .setPartitionStrategy(Tm.PartitionStrategy.HASH)
                    .addAllOutputMetadata(List.of(new Tm.OutputMetadata[]{
                            Tm.OutputMetadata.newBuilder()
                                    .setName("SvCountOperator_1-0")
                                    .setAddress(NodeBase.getHost()+":8018")
                                    .setPartitionPlan(
                                    Tm.PartitionPlan.newBuilder()
                                            .setPartitionStart(0)
                                            .setPartitionEnd(Integer.MAX_VALUE)
                                            .build()
                            ).build(),
                            Tm.OutputMetadata.newBuilder()
                                    .setName("SvCountOperator_1-1")
                                    .setAddress(NodeBase.getHost()+":8028")
                                    .setPartitionPlan(
                                    Tm.PartitionPlan.newBuilder()
                                            .setPartitionStart(Integer.MIN_VALUE)
                                            .setPartitionEnd(-1)
                                            .build()
                            ).build()
                    }));

            Tm.ReconfigMsg.Builder reconfigMsgBuilder = Tm.ReconfigMsg.newBuilder()
                    .putConfig(prevStageOpCfg.getName(), prevStageOpCfg.build())
                    .putConfig(oldOpCfgBuilder.getName(), oldOpCfgBuilder.build())
                    .putConfig(newOpCfgBuilder.getName(), newOpCfgBuilder.build())
                    .setEffectiveWaterMark(-1);
            sourceTMClient.sendReconfigControlMessage(
                    reconfigMsgBuilder.build().getConfigMap(),
                    reconfigMsgBuilder.build().getEffectiveWaterMark()
            );


        } catch (Exception e) {
            logger.error("tryScale", e);
            FatalUtil.fatal("tryScale", e);
        }
        return "\nok";
    }
}