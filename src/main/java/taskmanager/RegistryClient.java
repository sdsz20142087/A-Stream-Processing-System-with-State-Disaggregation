package taskmanager;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Cp;
import pb.RegistryServiceGrpc;

class RegistryClient {
    private final Logger logger = LogManager.getLogger();
    private final String target;
    private final RegistryServiceGrpc.RegistryServiceStub asyncStub;

    public RegistryClient(String host, int port) {
        target = host + ":" + port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = RegistryServiceGrpc.newStub(channel);
    }

    public void registerTM(String localAddress, String name) {
        logger.info("Registering TM at Control Plane");
        Cp.RegisterTMRequest req = Cp.RegisterTMRequest.newBuilder()
                .setAddress(localAddress)
                .setName(name)
                .setPort(TaskManager.TM_GRPC_PORT)
                .build();
        asyncStub.registerTM(req, new StreamObserver<>() {
            @Override
            public void onNext(Cp.RegisterTMResponse value) {
                logger.info("Got response: " + value);
            }

            @Override
            public void onError(Throwable t) {
                logger.fatal("Failed to register TM at Control Plane " + target, t);
                System.exit(1);

            }

            @Override
            public void onCompleted() {
                logger.info("registerTM Completed");
            }
        });
    }
}

