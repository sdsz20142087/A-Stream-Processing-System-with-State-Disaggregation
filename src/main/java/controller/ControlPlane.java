package controller;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.NodeBase;

import java.io.IOException;

public class ControlPlane extends NodeBase {
    public static final int CP_GRPC_PORT = 8001;
    private Logger logger = LogManager.getLogger();
    private final Server cpServer;
    private static ControlPlane instance;

    public static ControlPlane getInstance() {
        if (instance == null)
            instance = new ControlPlane();
        return instance;
    }

    private ControlPlane() {
        // start the grpc server on port 8001
        cpServer = ServerBuilder.forPort(CP_GRPC_PORT).addService(new RegistryServiceImpl()).build();
    }

    public void start() {
        try {
            this.cpServer.start();
            logger.info("ControlPlane started on " + CP_GRPC_PORT);
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
