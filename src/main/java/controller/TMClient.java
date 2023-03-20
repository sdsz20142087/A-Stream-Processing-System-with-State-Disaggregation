package controller;

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.CPServiceGrpc;
import pb.TMServiceGrpc;
import pb.Tm;
import stateapis.BaseState;
import stateapis.State;

import java.io.*;

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

    public void addOperator(Tm.OperatorConfig config, BaseOperator operator) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(operator);
        byte[] bytes = baos.toByteArray();
        ByteString bs = ByteString.copyFrom(bytes);
        Tm.AddOperatorRequest req = Tm.AddOperatorRequest.newBuilder().setConfig(config).setObj(bs).build();
        blockingStub.addOperator(req);
        logger.info("deployed operator " + operator.getName() + " bytes, size=" + bytes.length + "");
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

    public void addState(Tm.StateConfig config, BaseState state) throws Exception{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(state);
        byte[] bytes = baos.toByteArray();
        ByteString bs = ByteString.copyFrom(bytes);
        Tm.AddStateRequest req = Tm.AddStateRequest.newBuilder().setConfig(config).setObj(bs).build();
        blockingStub.addState(req);
    }

    public void removeState(Tm.StateConfig config) throws Exception{

        Tm.RemoveStateRequest req = Tm.RemoveStateRequest.newBuilder().build();
        blockingStub.removeState(req);
    }

    // get state from TM
    public State getState(String key) throws IOException, ClassNotFoundException {
        logger.info("get state from TM at " + host + ":" + port);
        Tm.GetStateRequest req = Tm.GetStateRequest.newBuilder().setStateKey(key).build();
        Tm.GetStateResponse res= blockingStub.getState(req);

        //deserialize
        byte[] bytes = res.getObj().toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois;

        ois = new ObjectInputStream(bis);
        State state = (State) ois.readObject();
        return state;
    }


}
