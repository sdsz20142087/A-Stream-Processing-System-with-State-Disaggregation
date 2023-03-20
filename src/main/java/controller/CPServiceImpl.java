package controller;

import DB.etcdDB.DBTools;
import io.grpc.stub.StreamObserver;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.CPServiceGrpc;
import pb.Cp;

import java.io.IOException;
import java.util.HashMap;

class CPServiceImpl extends CPServiceGrpc.CPServiceImplBase {
    private Logger logger = LogManager.getLogger();
    private HashMap<String, TMClient> tmClients = new HashMap<>();
    private DBTools dbTools = DBTools.getInstance();

    public HashMap<String, TMClient> getTMClients(){
        return tmClients;
    }

    @Override
    public void registerTM(Cp.RegisterTMRequest request,
                           StreamObserver<Cp.RegisterTMResponse> responseObserver) {
        logger.info("got registry from:" + request.getName() + "@" + request.getAddress() + ":" +request.getPort());
        Cp.RegisterTMResponse.Builder b = Cp.RegisterTMResponse.newBuilder();
        TMClient tmClient = new TMClient(request.getAddress(), request.getPort());
        tmClients.put(request.getName(),tmClient);

        String status = null;
        try {
            status = dbTools.registerTM(request.getName(), tmClient);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info("DB_status: " + status);

        dbTools.getTM(request.getName());

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

    @Override
    public void reportStatus(Cp.ReportQueueStatusRequest req,
                             StreamObserver<Cp.ReportQueueStatusResponse> responseObserver) {
        logger.info("get status push msg from " + req.getTmName() + ":" + req.getOpName());
        if (!tmClients.containsKey(req.getTmName())) {
            responseObserver.onError(new Exception("TM doesn't exist"));
        }
        Cp.ReportQueueStatusResponse.Builder responseBuilder = Cp.ReportQueueStatusResponse.newBuilder();
        responseBuilder.setStatus(ControlPlane.getInstance().reportTMStatus(req.getTmName(), req.getOpName(), req.getInputQueueLength()));
    }
}