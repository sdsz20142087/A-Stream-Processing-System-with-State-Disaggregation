package controller;

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;

import java.io.*;
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

    public TMClient(String completedAddress) {
        String target = completedAddress;
        String[] parts = target.split(":");
        String address = parts[0];
        int port = Integer.parseInt(parts[1]);
        this.host = address;
        this.port = port;
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

    public void addOperator(Tm.OperatorConfig config, BaseOperator operator) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(operator);
        byte[] bytes = baos.toByteArray();
        ByteString bs = ByteString.copyFrom(bytes);
        Tm.AddOperatorRequest req = Tm.AddOperatorRequest.newBuilder().setConfig(config).setObj(bs).build();
        logger.info("real: " + req.getConfig().getName());
        blockingStub.addOperator(req);
        logger.info("deployed operator " + operator.getOpName() + " bytes, size=" + bytes.length + "");
    }

    public void reConfigOp(Tm.OperatorConfig config) {
        Tm.ReConfigOperatorRequest req = Tm.ReConfigOperatorRequest.newBuilder().setConfig(config).build();
        blockingStub.reConfigOperator(req);
        logger.info("re-config operator " +config.getName());
    }

    public OperatorLoadBalancer.Pair<Integer, Integer> getOpStatus(String name) {
        Tm.OPStatusRequest req = Tm.OPStatusRequest.newBuilder().setName(name).build();
        final Tm.OperatorStatus[] return_value = new Tm.OperatorStatus[1];
        asyncStub.getOperatorStatus(req, new StreamObserver<>() {
            @Override
            public void onNext(Tm.OperatorStatus value) {
                return_value[0] = value;
                logger.info("Got response inputQueue length: " + value.getInputQueueLength());
            }

            @Override
            public void onError(Throwable t) {
                logger.fatal("Failed to get Operator status");
                System.exit(1);

            }

            @Override
            public void onCompleted() {
                logger.info("get operator status completed");
            }
        });
        return new OperatorLoadBalancer.Pair<>(return_value[0].getInputQueueLength(), return_value[0].getOutputQueueLength());
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getAddress(){
        return host + ":" + port;
    }

    public void removeState(String stateKey) throws IOException, ClassNotFoundException{
        logger.info("remove state from TM at " + host + ":" + port);

        Tm.RemoveStateRequest req = Tm.RemoveStateRequest.newBuilder().setStateKey(stateKey).build();
        blockingStub.removeState(req);
    }

    // get state from TM
    public Object getState(String stateKey) throws IOException, ClassNotFoundException {
        logger.info("get state from TM at " + host + ":" + port);
        Tm.GetStateRequest req = Tm.GetStateRequest.newBuilder().setStateKey(stateKey).build();
        Tm.GetStateResponse res= blockingStub.getState(req);

        //deserialize
        byte[] bytes = res.getObj().toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois;

        ois = new ObjectInputStream(bis);
        return ois.readObject();
    }

    public void updateState(String stateKey, Object state) throws IOException, ClassNotFoundException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(state);
        byte[] bytes = baos.toByteArray();
        ByteString bs = ByteString.copyFrom(bytes);
        Tm.UpdateStateRequest req = Tm.UpdateStateRequest.newBuilder().setStateKey(stateKey).setObj(bs).build();
        blockingStub.updateState(req);
    }

}
