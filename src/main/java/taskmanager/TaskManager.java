package taskmanager;

import config.Config;
import config.TMConfig;
import io.grpc.*;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import utils.NodeBase;

import java.io.IOException;
import java.util.List;

public class TaskManager extends NodeBase {
    private static TaskManager instance;
    private CPClient cpClient;
    private Server tmServer;
    private final TMConfig tmcfg;
    private TMServiceImpl tmService;

    private TaskManager() {
        // configure GRPC to use PickFirstLB
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        // read the config file
        Config.LoadConfig(configPath);

        tmcfg = Config.getInstance().taskManager;


    }

    public void init(String[] args) {

        try {
            int actualPort = tmcfg.tm_port;
            // parse cmdline args for -port=xxxx
            for (String arg : args) {
                if (arg.startsWith("-port=")) {
                    actualPort = Integer.parseInt(arg.substring(6));
                    logger.warn("TM: overriding port to " + actualPort + " from cmdline");
                    tmcfg.tm_port = actualPort;
                    tmcfg.rocksDBPath = "data-" + actualPort + ".db";
                    break;
                }
            }


            logger.info("tm_port=" + actualPort);
            cpClient = new CPClient(tmcfg.cp_host, tmcfg.cp_port, actualPort, tmcfg.useCache);

            // boot the service
            tmService = new TMServiceImpl(tmcfg, cpClient);
            tmServer = ServerBuilder.forPort(actualPort).addService(tmService).build();
            this.tmServer.start();

            // register at control plane
            logger.info("Registering at control plane=" + tmcfg.cp_host + ":" + tmcfg.cp_port);
            String localAddr = cpClient.registerTM(getHost(), getName());

            // This is a hack since we don't have a way to get the external address of
            // the TM before registration, but kvprovider needs our addr to access state
            tmService.setLocalAddr(localAddr);

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
        TaskManager.getInstance().init(args);
    }
}
