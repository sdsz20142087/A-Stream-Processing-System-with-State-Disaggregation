package controller;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Cp;
import pb.RegistryServiceGrpc;

import java.io.IOException;

public class ControlPlane {
    public static final int CP_GRPC_PORT = 8001;
    private Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        // start the grpc server on port 8081
        RegistryServer server = new RegistryServer(CP_GRPC_PORT);
        server.start();
    }
}


class RegistryServer {
    private final int port;
    private final Server server;
    private Logger logger = LogManager.getLogger();
    public RegistryServer(int port) {
        this.port = port;
        server = ServerBuilder.forPort(port).addService(new RegistryServiceImpl()).build();
    }

    public void start() {
        try {
            this.server.start();
            logger.info("Server started on " + port);
            // let this thread block until server termination
            this.server.awaitTermination();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class RegistryServiceImpl extends RegistryServiceGrpc.RegistryServiceImplBase {
    private Logger logger = LogManager.getLogger();
    @Override
    public void registerTM(Cp.RegisterTMRequest request,
                           StreamObserver<Cp.RegisterTMResponse> responseObserver) {
        logger.info("got request:" + request);
        responseObserver.onNext(Cp.RegisterTMResponse.newBuilder().setStatus("ok").build());
        responseObserver.onCompleted();
    }

    @Override
    public void deregisterTM(Cp.DeregisterTMRequest req,
                             StreamObserver<Cp.DeregisterTMResponse> responseObserver) {
        responseObserver.onError(new Exception("not implemented yet!"));
    }
}