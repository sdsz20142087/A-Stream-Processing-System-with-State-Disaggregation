package controller;

import DB.etcdDB.DBTools;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Cp;
import pb.RegistryServiceGrpc;

import java.io.IOException;
import java.util.HashMap;

class RegistryServiceImpl extends RegistryServiceGrpc.RegistryServiceImplBase {
    private Logger logger = LogManager.getLogger();
    private HashMap<String, TMClient> tmClients = new HashMap<>();

    private DBTools dbTools = DBTools.getInstance();

    @Override
    public void registerTM(Cp.RegisterTMRequest request,
                           StreamObserver<Cp.RegisterTMResponse> responseObserver) {
        logger.info("got registry from:" + request.getName() + "@" + request.getAddress() + ":" +request.getPort());
        Cp.RegisterTMResponse.Builder b = Cp.RegisterTMResponse.newBuilder();
        TMClient tmClient = new TMClient(request.getAddress(), request.getPort());
        tmClients.put(request.getName(),tmClient);
        logger.info("a");
        String status = null;
        try {
            status = dbTools.registerTM(request.getName(), tmClient);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("DB_status: " + status);
        logger.info("b");
        dbTools.getTM(request.getName());
        logger.info("c");
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