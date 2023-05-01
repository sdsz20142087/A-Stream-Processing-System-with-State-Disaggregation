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
import utils.BytesUtil;

import java.io.*;
import java.io.Serializable;
import java.util.Map;

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
        return resp.toString();
    }

    public void addOperator(Tm.OperatorConfig config, BaseOperator operator) throws Exception {

        byte[] bytes = BytesUtil.checkedObjectToBytes(operator);
        ByteString bs = ByteString.copyFrom(bytes);
        Tm.AddOperatorRequest req = Tm.AddOperatorRequest.newBuilder().setConfig(config).setObj(bs).build();
        blockingStub.addOperator(req);
        logger.info("deployed operator " + operator.getOpName() + " bytes, size=" + bytes.length + "");
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
        return BytesUtil.checkedObjectFromBytes(bytes);
    }

    public void updateState(String stateKey, Object state) throws IOException, ClassNotFoundException{
        byte[] bytes = BytesUtil.checkedObjectToBytes(state);

        ByteString bs = ByteString.copyFrom(bytes);
        Tm.UpdateStateRequest req = Tm.UpdateStateRequest.newBuilder().setStateKey(stateKey).setObj(bs).build();
        blockingStub.updateState(req);
    }

    public long getOperatorLowWatermark(String receiverOpName, String content) {
        Tm.OperatorLowWatermarkRequest req = Tm.OperatorLowWatermarkRequest.newBuilder().setName(receiverOpName).setContent(content).build();
        final Tm.OperatorLowWatermarkResponse[] return_value = new Tm.OperatorLowWatermarkResponse[1];
        asyncStub.getOperatorLowWatermark(req, new StreamObserver<>() {
            @Override
            public void onNext(Tm.OperatorLowWatermarkResponse value) {
                return_value[0] = value;
                logger.info("Got response lowWatermark: " + value.getLowWatermark());
            }

            @Override
            public void onError(Throwable t) {
                logger.fatal("Failed to get lowWatermark");
                System.exit(1);

            }

            @Override
            public void onCompleted() {
                logger.info("get operator lowWatermark completed");
            }
        });
        return return_value[0].getLowWatermark();
    }

    public void sendExternalControlMessage(String receiverOpName, long consistentTimeStamp, String content) {
        Tm.OperatorExternalTimestampRequest req = Tm.OperatorExternalTimestampRequest.newBuilder()
                .setName(receiverOpName)
                .setContent(content)
                .setReconfigTimestamp(consistentTimeStamp)
                .build();
        blockingStub.setOperatorExternalTimestamp(req);
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

    public void sendReconfigControlMessage(Map<String, Tm.OperatorConfig> operatorConfigMap, long watermark){
        // construct reconfig message
        Tm.ReconfigMsg.Builder reconfigMsgBuilder = Tm.ReconfigMsg.newBuilder();
        for (Map.Entry<String, Tm.OperatorConfig> entry : operatorConfigMap.entrySet()) {
            reconfigMsgBuilder.putConfig(entry.getKey(), entry.getValue());
        }

        reconfigMsgBuilder.setEffectiveWaterMark(watermark);
        Tm.ReconfigMsg reconfigMsg = reconfigMsgBuilder.build();
        // put reconfigMsg into pendingReconfigMsgs
        Tm.Msg msg = Tm.Msg.newBuilder()
                .setType(Tm.Msg.MsgType.CONTROL)
                .setReconfigMsg(reconfigMsg)
                .setIngestTime(-1)
                .setExtIngestTime(-1)
                .setReceiverOperatorName("SourceOperator_1-0")
                .build();
        Tm.MsgList msgList = Tm.MsgList.newBuilder().addMsgs(msg).build();
        blockingStub.pushMsgList(msgList);

    }
}
