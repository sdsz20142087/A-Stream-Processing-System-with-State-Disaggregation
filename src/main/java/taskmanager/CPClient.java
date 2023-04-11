package taskmanager;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.CPServiceGrpc;
import pb.Cp;

public class CPClient {
    private final Logger logger = LogManager.getLogger();
    private final String target;
    private final int tm_port;
    private final CPServiceGrpc.CPServiceStub asyncStub;
    private final CPServiceGrpc.CPServiceBlockingStub blockingStub;

    public CPClient(String host, int cp_port, int tm_port) {
        target = host + ":" + cp_port;
        this.tm_port = tm_port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = CPServiceGrpc.newStub(channel);
        blockingStub = CPServiceGrpc.newBlockingStub(channel);
    }

    // returns the external ip addr of this client
    public String registerTM(String localAddress, String name) {
        logger.info("Registering TM at Control Plane");
        Cp.RegisterTMRequest req = Cp.RegisterTMRequest.newBuilder()
                .setAddress(localAddress)
                .setName(name)
                .setPort(this.tm_port)
                .build();
        Cp.RegisterTMResponse resp = blockingStub.registerTM(req);
        logger.info("Got response: " + resp);
        return resp.getExternalAddress();
    }

    public String getState(String keyPrefix) {
        logger.info("Getting state TM addr from Control Plane");
        Cp.FindRemoteStateAddressRequest req = Cp.FindRemoteStateAddressRequest.
                newBuilder().setStateKey(keyPrefix).build();
        Cp.FindRemoteStateAddressResponse res;
        res = blockingStub.findRemoteStateAddress(req);
        return res.getAddress();
    }
}

