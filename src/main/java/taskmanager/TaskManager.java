package taskmanager;

import config.Config;
import config.TMConfig;
import io.grpc.*;
import utils.NodeBase;
import java.io.IOException;

import static java.lang.Thread.sleep;

public class TaskManager extends NodeBase {
    private static TaskManager instance;
    private final CPClient registryClient;
    private final Server tmServer;
    private final TMConfig tmcfg;
    private final TMServiceImpl tmService;

    private TaskManager(int grpcPort) {

        // read the config file
        Config.LoadConfig(configPath);

        tmcfg = Config.getInstance().taskManager;

        // !!!!tm_port is assigned at runtime to make things simple
        tmcfg.tm_port = grpcPort;

        logger.info("tm_port=" + Config.getInstance().taskManager.tm_port);
        registryClient = new CPClient(tmcfg.cp_host, tmcfg.cp_port, tmcfg.tm_port);
        tmService = new TMServiceImpl(tmcfg.operator_quota);
        tmServer = ServerBuilder.forPort(tmcfg.tm_port).addService(tmService).build();
    }

    public void init() {
        try {
            // register at control plane
            logger.info("Registering at control plane=" + tmcfg.cp_host + ":" + tmcfg.cp_port);
            logger.info(getHost());
            registryClient.registerTM(getHost(), getName());
            this.tmServer.start();
            logger.info("TaskManager started on " + tmcfg.tm_port);
            // let this thread block until server termination

            this.tmServer.awaitTermination();
        } catch (IOException | InterruptedException e) {
            logger.fatal("Failed to start TaskManager", e);
            System.exit(1);
        }
    }

    public static TaskManager getInstance() {
        if (instance == null)
            instance = new TaskManager(8010);
        return instance;
    }

    public static void main(String[] args) {
        TaskManager.getInstance().init();
    }
}
