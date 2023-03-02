package controller;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;

import java.io.Serializable;

public class TMClient implements Serializable {
    private final Logger logger = LogManager.getLogger();
    private final TMServiceGrpc.TMServiceStub asyncStub;
    private final TMServiceGrpc.TMServiceBlockingStub blockingStub;
    private final String host;
    private final int port;

    public TMClient(String address, int port) {
        this.host = address;
        this.port = port;
        String target = address + ":" + port;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        asyncStub = TMServiceGrpc.newStub(channel);
        blockingStub = TMServiceGrpc.newBlockingStub(channel);
    }

    public String getStatus() throws Exception {
        logger.info("Getting status from TM at " + host + ":" + port);
        Tm.TMStatusRequest req = Tm.TMStatusRequest.newBuilder().build();
        Tm.TMStatusResponse resp = blockingStub.getStatus(req);
        logger.info("Got response: " + resp);
//        asyncStub.getStatus(req, new StreamObserver<Tm.TMStatusResponse>() {
//            @Override
//            public void onNext(Tm.TMStatusResponse value) {
//                logger.info("Got response: " + value);
//            }
//
//            @Override
//            public void onError(Throwable t) {
//                logger.fatal("Failed to get status from TM at " + host + ":" + port, t);
//            }
//
//            @Override
//            public void onCompleted() {
//                logger.info("getStatus Completed");
//            }
//        });
        return resp.toString();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
