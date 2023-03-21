package controller;

import DB.etcdDB.DBTools;
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
    private final CPConfig cpcfg;
    private static ControlPlane instance;

    private final Scheduler scheduler;

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
        DBTools.init(cpcfg.etcd_endpoints, cpcfg.test_etcd_conn);

        CPServiceImpl svc = new CPServiceImpl();

        // start the grpc server
        cpServer = ServerBuilder.forPort(cpcfg.cp_grpc_port)
                .addService(svc).build();
        logger.info("ControlPlane gRPC server on port " + cpcfg.cp_grpc_port);

        // start the scheduler thread
        this.scheduler = new Scheduler(App.getInstance().getQueryPlan(), svc.getTMClients());
        logger.info("ControlPlane scheduler init");
    }

    public void init() {
        try {
            this.cpServer.start();
            logger.info("ControlPlane gRPC server started");
            logger.info("ControlPlane scheduler starting in 5s...");
            Thread.sleep(5000);
            this.scheduler.start();
            logger.info("ControlPlane scheduler started");
            // let this thread block until server termination
            this.cpServer.awaitTermination();
        } catch (IOException | InterruptedException e) {
            logger.fatal("Failed to start ControlPlane", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        ControlPlane.getInstance().init();
    }
}
