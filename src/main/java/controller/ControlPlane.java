package controller;

import config.CPConfig;
import config.Config;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import utils.NodeBase;
import java.io.IOException;
import java.util.Arrays;

public class ControlPlane extends NodeBase {
    private final Server cpServer;
//    private final KV kvClient;
    private final CPConfig cpcfg;
    private static ControlPlane instance;

    public static ControlPlane getInstance() {
        if (instance == null)
            instance = new ControlPlane();
        return instance;
    }

    private ControlPlane() {
        // read the config file
        Config.LoadConfig(configPath);
        cpcfg = Config.getInstance().controlPlane;

        // initialize the etcd client
//        Client client = Client.builder().endpoints(cpcfg.etcd_endpoints).build();
//        this.kvClient = client.getKVClient();
//        if (cpcfg.test_etcd_conn){
//            // test the connection
//            try {
//                this.kvClient.get(ByteSequence.from("test_key".getBytes())).get();
//                logger.info("Connected to etcd on"+ Arrays.toString(cpcfg.etcd_endpoints));
//            } catch (Exception e) {
//                logger.fatal("Failed to connect to etcd", e);
//                System.exit(1);
//            }
//        }
//        logger.info("initialized etcd kvclient, endpoints="+Arrays.toString(cpcfg.etcd_endpoints));

        // start the grpc server
        cpServer = ServerBuilder.forPort(cpcfg.cp_grpc_port)
                .addService(new RegistryServiceImpl()).build();
        logger.info("ControlPlane gRPC server started on port " + cpcfg.cp_grpc_port);
    }

    public void start() {
        try {
            this.cpServer.start();
            // let this thread block until server termination
            this.cpServer.awaitTermination();
        } catch (IOException | InterruptedException e) {
            logger.fatal("Failed to start ControlPlane", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        ControlPlane.getInstance().start();
    }
}
