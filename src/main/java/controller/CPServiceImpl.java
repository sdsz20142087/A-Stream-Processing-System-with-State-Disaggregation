package controller;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.CPServiceGrpc;
import pb.Cp;

import java.util.HashMap;
import java.util.Random;

class CPServiceImpl extends CPServiceGrpc.CPServiceImplBase {
    private final Logger logger = LogManager.getLogger();
    private HashMap<String, TMClient> tmClients = new HashMap<>();

    public HashMap<String, TMClient> getTMClients(){
        return tmClients;
    }
    private HashMap<String, String> RoutingTable = new HashMap<>();
    private ConsistentHash consistentHash = new ConsistentHash(3);

    @Override
    public void registerTM(Cp.RegisterTMRequest request,
                           StreamObserver<Cp.RegisterTMResponse> responseObserver) {
        logger.info("got registry from:" + request.getName() + "@" + request.getAddress() + ":" +request.getPort());
        Cp.RegisterTMResponse.Builder b = Cp.RegisterTMResponse.newBuilder();
        TMClient tmClient = new TMClient(request.getAddress(), request.getPort());
        tmClients.put(request.getName(),tmClient);
        logger.info("a");

        try{
            logger.info("status:"+tmClient.getStatus());
            b.setStatus("ok");
            ControlPlane.getInstance().tmClientCnt++;
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
    public void findRemoteStateAddress(Cp.FindRemoteStateAddressRequest req,
                                         StreamObserver<Cp.FindRemoteStateAddressResponse> responseObserver){
        Cp.FindRemoteStateAddressResponse.Builder b = Cp.FindRemoteStateAddressResponse.newBuilder();
        if(!RoutingTable.containsKey(req.getStateKey())){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("state address not found")));
            return;
        }
        try {
            String address = RoutingTable.get(req.getStateKey());
            b.setAddress(address);
        } catch (Exception e) {
            String msg = String.format("can not find state address in routing table");
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        Cp.FindRemoteStateAddressResponse response = b.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void addRemoteStateAddress(Cp.AddRemoteStateAddressRequest req,
                                        StreamObserver<Cp.AddRemoteStateAddressResponse> responseObserver){
        Cp.AddRemoteStateAddressResponse.Builder b = Cp.AddRemoteStateAddressResponse.newBuilder();
        // find a random TM
        String[] keys= tmClients.keySet().toArray(new String[0]);
        Random random = new Random();
        String randomKey = keys[random.nextInt(keys.length)];
        TMClient tmClient = tmClients.get(randomKey);
        String randomAddress = tmClient.getHost();
        if(RoutingTable.containsKey(req.getStateKey())){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("state already exist")));
            return;
        }
        try {
            RoutingTable.put(req.getStateKey(), randomAddress);
        } catch (Exception e) {
            String msg = String.format("can not add state address in routing table");
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    public void removeRemoteStateAddress(Cp.RemoveRemoteStateAddressRequest req,
                                         StreamObserver<Cp.RemoveRemoteStateAddressResponse> responseObserver){
        Cp.RemoveRemoteStateAddressResponse.Builder b = Cp.RemoveRemoteStateAddressResponse.newBuilder();
        if(!RoutingTable.containsKey(req.getStateKey())){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("state not found")));
            return;
        }
        try {
            RoutingTable.remove(req.getStateKey());
        } catch (Exception e) {
            String msg = String.format("can not remove state address from routing table");
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    public void updateRemoteStateAddress(Cp.UpdateRemoteStateAddressRequest req,
                                         StreamObserver<Cp.UpdateRemoteStateAddressResponse> responseObserver){
        Cp.UpdateRemoteStateAddressResponse.Builder b = Cp.UpdateRemoteStateAddressResponse.newBuilder();
        if(!RoutingTable.containsKey(req.getStateKey())){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("state not found")));
            return;
        }
        try {
            String stateKey = req.getStateKey();
            String address =req.getAddress();
            RoutingTable.replace(stateKey,address);
        } catch (Exception e) {
            String msg = String.format("can not update state address in routing table");
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    public void getConsistentAddress(Cp.GetConsistentAddressRequest req,
                                     StreamObserver<Cp.GetConsistentAddressResponse> responseObserver) {
        Cp.GetConsistentAddressResponse.Builder b = Cp.GetConsistentAddressResponse.newBuilder();
        try {
            String address = consistentHash.get(req.getKey());
            if (address == null) {
                throw new Exception("state address not found");
            }
            b.setAddress(address);
        } catch (Exception e) {
            String msg = String.format("can not find state address in routing table: %s", e.getMessage());
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        Cp.GetConsistentAddressResponse response = b.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void addConsistentAddress(Cp.AddConsistentAddressRequest req,
                                     StreamObserver<Cp.AddConsistentAddressResponse> responseObserver){
        Cp.AddConsistentAddressResponse.Builder b = Cp.AddConsistentAddressResponse.newBuilder();
        // find a random TM
        String[] keys= tmClients.keySet().toArray(new String[0]);
        Random random = new Random();
        String randomKey = keys[random.nextInt(keys.length)];
        TMClient tmClient = tmClients.get(randomKey);
        String randomAddress = tmClient.getHost();

        try {
            consistentHash.add(req.getKey(), randomAddress);
        } catch (Exception e) {
            String msg = String.format("can not add state address in routing table");
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    public void removeConsistentAddress(Cp.RemoveConsistentAddressRequest req,
                                        StreamObserver<Cp.RemoveConsistentAddressResponse> responseObserver){
        Cp.RemoveConsistentAddressResponse.Builder b = Cp.RemoveConsistentAddressResponse.newBuilder();
        try {
            consistentHash.remove(req.getKey());
        } catch (Exception e) {
            String msg = String.format("can not remove state address from routing table");
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }
}