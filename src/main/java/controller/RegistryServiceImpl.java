package controller;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Cp;
import pb.RegistryServiceGrpc;

import java.util.HashMap;

class RegistryServiceImpl extends RegistryServiceGrpc.RegistryServiceImplBase {
    private Logger logger = LogManager.getLogger();
    private HashMap<String, TMClient> tmClients = new HashMap<>();

    @Override
    public void registerTM(Cp.RegisterTMRequest request,
                           StreamObserver<Cp.RegisterTMResponse> responseObserver) {
        logger.info("got registry from:" + request.getName() + "@" + request.getAddress());
        Cp.RegisterTMResponse.Builder b = Cp.RegisterTMResponse.newBuilder();
        TMClient tmClient = new TMClient(request.getAddress(), request.getPort());
        tmClients.put(request.getName(),tmClient);
        try{
            logger.info("status:"+tmClient.getStatus());
            b.setStatus("ok");
        }catch (Exception e){
            logger.fatal("Failed to get status from TM at " + request.getAddress(), e);
            b.setStatus("failed");
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deregisterTM(Cp.DeregisterTMRequest req,
                             StreamObserver<Cp.DeregisterTMResponse> responseObserver) {
        responseObserver.onError(new Exception("not implemented yet!"));
    }
}