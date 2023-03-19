package controller;

import DB.etcdDB.DBTools;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.CPServiceGrpc;
import pb.Cp;
import stateapis.State;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

class CPServiceImpl extends CPServiceGrpc.CPServiceImplBase {
    private Logger logger = LogManager.getLogger();
    private HashMap<String, TMClient> tmClients = new HashMap<>();

    private DBTools dbTools = DBTools.getInstance();

    public HashMap<String, TMClient> getTMClients(){
        return tmClients;
    }
    private HashMap<String, String> RoutingTable = new HashMap<>();

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

    public String findRemoteStateAddress(String stateKey){
        return RoutingTable.get(stateKey);
    }

    public String addRemoteStateAddress(Cp.addRemoteStateRequest req,
                                        StreamObserver<Cp.addRemoteStateResponse> responseObserver){
        // find a random TM
        String[] keys= tmClients.keySet().toArray(new String[0]);
        Random random = new Random();
        String randomKey = keys[random.nextInt(keys.length)];
        TMClient tmClient = tmClients.get(randomKey);
        String randomAddress = tmClient.getHost();
        RoutingTable.put(req.getStateKey(), randomAddress);
        // TODO: put state into TM
        tmClient.addState(req.getStateKey(), req.getState());
        return randomAddress;
    }

    public void removeRemoteStateAddress(Cp.removeRemoteStateRequest req,
                                         StreamObserver<Cp.removeRemoteStateResponse> responseObserver){
        String address = RoutingTable.get(req.getStateKey());
        RoutingTable.remove(req.getStateKey());
        // TODO: remove state from TM
        TMClient tmClient= tmClients.get(address);
        tmClient.removeState(req.getStateKey());
    }

    public State getRemoteState(Cp.getRemoteStateRequest req,
                                StreamObserver<Cp.getRemoteStateResponse> responseObserver){
        String address = RoutingTable.get(req.getStateKey());
        TMClient tmClient= tmClients.get(address);
        // TODO: get state from TM
        return tmClient.getState(req.getStateKey());
    }

}