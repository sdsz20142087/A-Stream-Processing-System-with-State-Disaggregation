package taskmanager;

import controller.ControlPlane;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.NodeBase;

import java.io.IOException;

public class TaskManager extends NodeBase {
    private static TaskManager instance;
    private Logger logger = LogManager.getLogger();
    public static final String CP_HOST = "127.0.0.1";
    public static final int TM_GRPC_PORT = 8002;
    private final RegistryClient registryClient;
    private final Server tmServer;

    private TaskManager() {
        registryClient = new RegistryClient(CP_HOST, ControlPlane.CP_GRPC_PORT);
        tmServer = ServerBuilder.forPort(TM_GRPC_PORT).addService(new TMServiceImpl()).build();
    }

    public void start() {
        try {
            // register at control plane
            logger.info("Registering at control plane="+CP_HOST+":"+ControlPlane.CP_GRPC_PORT);
            registryClient.registerTM(getHost(),getName());
            this.tmServer.start();
            logger.info("TaskManager started on " + TM_GRPC_PORT);
            // let this thread block until server termination
            this.tmServer.awaitTermination();
        } catch (IOException | InterruptedException e) {
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
        TaskManager.getInstance().start();
    }
}
