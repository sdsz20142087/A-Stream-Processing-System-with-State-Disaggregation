package taskmanager;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import config.TMConfig;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import kotlin.Pair;
import kotlin.Triple;
import operators.BaseOperator;
import operators.StateDescriptorProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;
import stateapis.*;
import utils.BytesUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

class TMServiceImpl extends TMServiceGrpc.TMServiceImplBase implements StateDescriptorProvider {
    private final int operatorQuota;
    private final HashMap<String, BaseOperator> operators;

    private final KVProvider kvProvider;
    private final Logger logger = LogManager.getLogger();

    // map of< TM's address, PushMsgClient>
    private final Map<String, PushMsgClient> pushMsgClients = new HashMap<>();

    private final Map<String, LinkedBlockingQueue<Tm.Msg>> opInputQueues = new HashMap<>();

    // map of <operator name, message>
    private final LinkedBlockingQueue<Triple<String, ByteString, Pair<Integer, Tm.Msg.MsgType>>> msgQueue = new LinkedBlockingQueue<>();

    private final HashMap<BaseOperator, Integer> roundRobinCounter = new HashMap<>();

    private TMConfig tmConfig;

    public void setLocalAddr(String addr){
        this.kvProvider.setLocalAddr(addr);
    }

    public TMServiceImpl(TMConfig tmcfg, CPClient cpClient) {
        super();
        this.tmConfig = tmcfg;
        KVProvider localKVProvider = new LocalKVProvider(tmcfg.rocksDBPath);
        this.kvProvider = tmcfg.useHybrid ?
                new HybridKVProvider(localKVProvider, cpClient, tmcfg.useMigration)
                : localKVProvider;
        logger.info("State config: using " + this.kvProvider.getClass().getName());
        this.operatorQuota = tmcfg.operator_quota;
        operators = new HashMap<>();
        logger.info("TM service started with operator quota: " + operatorQuota);
        // boot the sendloop
        new Thread(() -> {
            try {
                this.sendLoop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    public void getStatus(Tm.TMStatusRequest request,
                          StreamObserver<Tm.TMStatusResponse> responseObserver) {
        logger.info("got status request");
        Tm.TMStatusResponse.Builder b = Tm.TMStatusResponse.newBuilder();
        b.setOperatorCount(this.operators.size());
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    public void getOperatorStatus(Tm.OPStatusRequest request,
                                  StreamObserver<Tm.OperatorStatus> responseObserver) {
        logger.info("got operator status query request");
        Tm.OperatorStatus.Builder status = Tm.OperatorStatus.newBuilder();
        BaseOperator baseOperator = operators.get(request.getName());
        status.setInputQueueLength(baseOperator.getInputQueueLength())
                .setOutputQueueLength(baseOperator.getOutputQueueLength())
                .setName(baseOperator.getOpName());
        responseObserver.onNext(status.build());
        responseObserver.onCompleted();
    }

    /**
     *
     */

    // the control plane sends over serialized operator,
    // we can just deserialize it and add it to the operators map
    private void initOperator(Tm.AddOperatorRequest request) throws IOException, ClassNotFoundException {
        byte[] bytes = request.getObj().toByteArray();
        BaseOperator op = (BaseOperator) BytesUtil.checkedObjectFromBytes(bytes);
        LinkedBlockingQueue<Tm.Msg> inputQueue = new LinkedBlockingQueue<>();
        // TODO: BEFORE OPERATOR BOOTS, TM SHOULD UPDATE ROUTING-TABLE IF NEEDED

        // FIXME: IMPLEMENT THIS
        if(this.tmConfig.useHybrid){
            // TODO: WRITE THINGS TO ROUTING TABLE
        }

        // the queues must be initialized before the operator starts
        op.init(request.getConfig(), inputQueue, msgQueue, this);
        op.postInit();
        op.start();
        if(request.getConfig().getPartitionStrategy() == Tm.PartitionStrategy.ROUND_ROBIN){
            roundRobinCounter.put(op, 0);
        }
        this.opInputQueues.put(op.getOpName(), inputQueue);
        operators.put(op.getOpName(), op);
        logger.info(String.format("Started operator %s --all: %s", op.getOpName(), operators.keySet()));

        // initialize the operator's pushmsg client if needed
        for(Tm.OutputMetadata meta: request.getConfig().getOutputMetadataList()){
            if(!pushMsgClients.containsKey(meta.getAddress())){
                pushMsgClients.put(meta.getAddress(), new PushMsgClient(logger, meta.getAddress()));
            }
        }
    }

    @Override
    public synchronized void addOperator(Tm.AddOperatorRequest request,
                            StreamObserver<Empty> responseObserver) {
        if (operators.size() >= operatorQuota) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator quota exceeded")));
            return;
        }
        logger.info("Display operators why key exists");
        for (String key : operators.keySet())  {
            logger.info(key);
        }
        logger.info("Finish");

        if (operators.containsKey(request.getConfig().getName())) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator already exists")));
            return;
        }
        logger.info(String.format("adding operator %d/%d",this.operators.size()+1,this.operatorQuota));
        try{
            initOperator(request);
        } catch (IOException | ClassNotFoundException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("failed to initialize operator")));
            return;
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void pushMsg(Tm.Msg request, StreamObserver<Empty> responseObserver) {
        String opName = request.getOperatorName();
        logger.info("got pushMsg request for "+opName);
        if(!operators.containsKey(opName)){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator "+opName+" not found")));
            return;
        }
        try {
            opInputQueues.get(opName).put(request);
        } catch (InterruptedException e) {
            String msg = String.format("interrupted while pushing message to %s", opName);
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     *
     */
    @Override
    public void removeOperator(Tm.RemoveOperatorRequest request,
                               StreamObserver<Empty> responseObserver) {

    }

    @Override
    public void reConfigOperator(Tm.ReConfigOperatorRequest request,
                                 StreamObserver<Empty> responseObserver) {
        Tm.OperatorConfig config = request.getConfig();
        try {
            operators.get(config.getName()).setConfig(config);

        } catch (Exception e) {
            String msg = "invalid op name.";
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }


    @Override
    public void getState(Tm.GetStateRequest request, StreamObserver<Tm.GetStateResponse> responseObserver){
        String stateKey = request.getStateKey();
        // note that we're assuming that if a state is remote, then it must exist, thus could not be null anyways
        Object state = this.kvProvider.get(stateKey, null);
        ByteString stateBytes = null;
        try {
            stateBytes = ByteString.copyFrom(BytesUtil.checkedObjectToBytes(state));
        } catch (IOException e) {
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription("Failed to serialize state object")));
            return;
        }
        Tm.GetStateResponse response = Tm.GetStateResponse.newBuilder().setObj(stateBytes).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }

    @Override
    public void removeState(Tm.RemoveStateRequest request, StreamObserver <Empty> responseObserver){
        try {
            String stateKey = request.getStateKey();
            kvProvider.delete(stateKey);
        } catch (Exception e) {
            String msg = String.format("can not remove state in TM");
            logger.error(msg);
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
            return;
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    // EQUIVALENT TO "PUT"
    public void updateState(Tm.UpdateStateRequest request, StreamObserver <Empty> responseObserver){
        String stateKey = request.getStateKey();
        byte[] stateBytes = request.getObj().toByteArray();
        kvProvider.put(stateKey, stateBytes);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }


    private void sendLoop() throws InterruptedException {
        while (true) {
            // operator-name, serialized msg, partition key
            Triple<String, ByteString, Pair<Integer, Tm.Msg.MsgType>> item = msgQueue.take();
            String opName = item.getFirst();
            BaseOperator op = operators.get(opName);
            Tm.OperatorConfig config = op.getConfig();
            List<Tm.OutputMetadata> targetOutput = new ArrayList<>();
            // apply the partition strategy
            if (item.getThird().getSecond() == Tm.Msg.MsgType.WATERMARK) {
                targetOutput = config.getOutputMetadataList();
                logger.info("WATERMARK BROADCAST");
            } else {
                switch (config.getPartitionStrategy()) {
                    case ROUND_ROBIN:
                        int val = roundRobinCounter.get(op);
                        int outputListLen = config.getOutputMetadataList().size();
                        //logger.info("sendloop: roundrobin counter for "+opName+" is "+val+" and output list len is "+outputListLen);
                        roundRobinCounter.put(op, val+1);
                        targetOutput.add(config.getOutputMetadataList().get(val % outputListLen));
                        break;
                    case HASH:
                        int outputIndex = item.getThird().getFirst() % config.getOutputMetadataList().size();
                        targetOutput.add(config.getOutputMetadataList().get(outputIndex));
                        break;
                    case BROADCAST:
                        targetOutput = config.getOutputMetadataList();
                        break;
                }
            }
            ByteString msgBytes = item.getSecond();
            Tm.Msg.Builder msgBuilder = Tm.Msg.newBuilder()
                    .setType(Tm.Msg.MsgType.DATA)
                    .setData(msgBytes);
            for(Tm.OutputMetadata target: targetOutput){
                Tm.Msg msg = msgBuilder.setOperatorName(target.getName()).build();
                pushMsgClients.get(target.getAddress()).pushMsg(msg);
            }
            logger.debug("sendloop: sending msg to"+targetOutput);
        }
    }

    @Override
    public ValueStateAccessor getValueStateAccessor(BaseOperator op, String stateName, Object defaultValue) {
        checkStateName(stateName);
        String stateDescriptor = op.getOpName() + "." + stateName;
        return new ValueStateAccessor(stateDescriptor, this.kvProvider, defaultValue);
    }

    @Override
    public MapStateAccessor getMapStateAccessor(BaseOperator op, String stateName) {
        checkStateName(stateName);
        return new MapStateAccessor(op.getOpName() + "." + stateName, this.kvProvider);
    }

    @Override
    public ListStateAccessor getListStateAccessor(BaseOperator op, String stateName) {
        checkStateName(stateName);
        return new ListStateAccessor(op.getOpName() + "." + stateName, this.kvProvider);
    }

    private void checkStateName(String name){
        if(name.contains(".")){
            throw new IllegalArgumentException("state name can not contain '.'");
        }
    }
}