package taskmanager;

import config.Config;
import config.TMConfig;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.NodeBase;

import java.io.IOException;

public class TaskManager extends NodeBase {
    private static TaskManager instance;
    private final RegistryClient registryClient;
    private final Server tmServer;
    private final TMConfig tmcfg;

    private TaskManager(int grpcPort) {

        // read the config file
        Config.LoadConfig(configPath);

        tmcfg = Config.getInstance().taskManager;
        tmcfg.tm_port = grpcPort;
        logger.info("tm_port=" + Config.getInstance().taskManager.tm_port);
        registryClient = new RegistryClient(tmcfg.cp_host, tmcfg.cp_port, tmcfg.tm_port);
        tmServer = ServerBuilder.forPort(tmcfg.tm_port).addService(new TMServiceImpl()).build();
    }

    public void start() {
        try {
            // register at control plane
            logger.info("Registering at control plane=" + tmcfg.cp_host + ":" + tmcfg.cp_port);
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
        TaskManager.getInstance().start();
    }
}
