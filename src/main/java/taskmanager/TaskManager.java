package taskmanager;

import config.Config;
import config.TMConfig;
import io.grpc.*;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import stateapis.KVProvider;
import stateapis.LocalKVProvider;
import stateapis.RemoteKVProvider;
import utils.NodeBase;

import java.io.IOException;
import java.util.List;

public class TaskManager extends NodeBase {
    private static TaskManager instance;
    private final CPClient registryClient;
    private final Server tmServer;
    private final TMConfig tmcfg;
    private final TMServiceImpl tmService;

    private TaskManager() {
        // configure GRPC to use PickFirstLB
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        // read the config file
        Config.LoadConfig(configPath);

        tmcfg = Config.getInstance().taskManager;

        int actualPort = tmcfg.tm_port;

        logger.info("tm_port=" + actualPort);
        registryClient = new CPClient(tmcfg.cp_host, tmcfg.cp_port, actualPort);
        KVProvider kvProvider = tmcfg.useHybrid ? new RemoteKVProvider() : new LocalKVProvider();
        logger.info("State config: using " + kvProvider.getClass().getName());
        tmService = new TMServiceImpl(tmcfg.operator_quota, kvProvider);
        tmServer = ServerBuilder.forPort(actualPort).addService(tmService).build();
    }

    public void init() {

        try {
            logger.info("TaskManager will start in 3 seconds，tm_port=" +tmcfg.tm_port);
            Thread.sleep(3000);
            // register at control plane
            logger.info("Registering at control plane=" + tmcfg.cp_host + ":" + tmcfg.cp_port);
            logger.info(getHost());
            registryClient.registerTM(getHost(), getName());
            this.tmServer.start();
            logger.info("TaskManager started on " + tmcfg.tm_port);
            // let this thread block until server termination

            this.tmServer.awaitTermination();
        } catch (IOException | InterruptedException e) {
            logger.warn(List.of(e.getStackTrace()));
            logger.fatal("Failed to start TaskManager", e);
            System.exit(1);
        }
    }

    public static TaskManager getInstance() {
        if (instance == null)
            instance = new TaskManager();
        return instance;
    }


    public static void main(String[] args) {
        TaskManager.getInstance().init();
    }
}
