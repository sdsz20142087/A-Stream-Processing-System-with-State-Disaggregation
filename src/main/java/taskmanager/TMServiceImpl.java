package taskmanager;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import config.TMConfig;
import controller.TMClient;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import operators.BaseOperator;
import operators.OutputMessage;
import operators.SourceOperator;
import operators.StateDescriptorProvider;
import operators.stateful.ServerCountOperator;
import operators.stateful.StatefulCPUHeavyOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;
import stateapis.*;
import utils.BytesUtil;
import utils.FatalUtil;
import utils.KeyUtil;

import java.io.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

class TMServiceImpl extends TMServiceGrpc.TMServiceImplBase implements StateDescriptorProvider, IStateMigration {
    private final int operatorQuota;
    private final HashMap<String, BaseOperator> operators;
    private int maxSeenReconfig = 0;
    private final KVProvider localKVProvider;
    private final KVProvider kvProvider;
    private final Logger logger = LogManager.getLogger();

    // map of< TM's address, PushMsgClient>
    private final Map<String, PushMsgClient> pushMsgClients = new HashMap<>();

    private final Map<String, LinkedBlockingQueue<Tm.Msg>> opInputQueues = new HashMap<>();

    // map of <operator name, message>
    private final LinkedBlockingQueue<OutputMessage> msgQueue = new LinkedBlockingQueue<>();

    private final HashMap<BaseOperator, Integer> roundRobinCounter = new HashMap<>();

    private final Queue<Tm.ReconfigMsg> pendingReconfigMsgs = new LinkedList<>();

    private final HashMap<String, List<Tm.Msg>> pendingOutputMsgs = new HashMap<>();

    private TMConfig tmConfig;

    public static final long WATERMARK_NOW = -1;

    public void setLocalAddr(String addr) {
        this.kvProvider.setLocalAddr(addr);
    }

    public TMServiceImpl(TMConfig tmcfg, CPClient cpClient) {
        super();
        this.tmConfig = tmcfg;
        KVProvider localKVProvider = new LocalKVProvider(tmcfg.rocksDBPath);
        this.localKVProvider = localKVProvider;
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
    private BaseOperator initOperator(Tm.AddOperatorRequest request) throws IOException, ClassNotFoundException {
        logger.info("========================================== init: {}", request.getConfig().getName())   ;
        byte[] bytes = request.getObj().toByteArray();
        BaseOperator op = (BaseOperator) BytesUtil.checkedObjectFromBytes(bytes);
        LinkedBlockingQueue<Tm.Msg> inputQueue = new LinkedBlockingQueue<>();
        // TODO: BEFORE OPERATOR BOOTS, TM SHOULD UPDATE ROUTING-TABLE IF NEEDED

        // FIXME: IMPLEMENT THIS
        if (this.tmConfig.useHybrid) {
            // TODO: WRITE THINGS TO ROUTING TABLE
        }
        if(!request.getConfig().getNoOverride()){
            // if stateful, reset its own partition plan
            if(op instanceof StatefulCPUHeavyOperator || op instanceof ServerCountOperator){

                Tm.OperatorConfig newCfg = request.getConfig().toBuilder().setPartitionPlan(
                        Tm.PartitionPlan.newBuilder().setPartitionStart(Integer.MIN_VALUE)
                                .setPartitionEnd(Integer.MAX_VALUE).build()
                ).build();
                logger.warn("Resetting hash partition: {}", newCfg.getPartitionPlan());
                request = request.toBuilder().mergeFrom(request).setConfig(newCfg).build();
            }
            // if hash: reset the output partition plan
            if(request.getConfig().getPartitionStrategy() == Tm.PartitionStrategy.HASH){
                List<Tm.OutputMetadata> outputs = request.getConfig().getOutputMetadataList();
                List<Tm.OutputMetadata> newOutputs = new ArrayList<>();
                for(int i = 0; i < outputs.size(); i++){
                    // range len should be int32 divided by N (number of partitions)
                    int rangeLen = (int) ((long) Math.pow(2, 32) / outputs.size());
                    int start = Integer.MIN_VALUE + i * rangeLen;
                    int end = Integer.MIN_VALUE + (i + 1) * rangeLen - 1;
                    if(i == outputs.size() - 1){
                        end = Integer.MAX_VALUE;
                    }
                    Tm.PartitionPlan plan = Tm.PartitionPlan.newBuilder().setPartitionStart(start).setPartitionEnd(end).build();
                    newOutputs.add(outputs.get(i).toBuilder().setPartitionPlan(plan).build());
                }

                Tm.OperatorConfig newCfg = request.getConfig().toBuilder().addAllOutputMetadata(newOutputs).build();
                logger.warn("Resetting outputmetatdata: {}", newCfg.getOutputMetadataList());
                request = request.toBuilder().mergeFrom(request).setConfig(newCfg).build();
            }
        }
        // the queues must be initialized before the operator starts
        op.init(request.getConfig(), inputQueue, msgQueue, this, this);
        op.postInit();
        op.start();
        if (request.getConfig().getPartitionStrategy() == Tm.PartitionStrategy.ROUND_ROBIN) {
            roundRobinCounter.put(op, 0);
        }
        this.opInputQueues.put(op.getOpName(), inputQueue);
        operators.put(op.getOpName(), op);
        logger.info(String.format("Started operator [%d] %s --all: %s", op.getConfig().getLogicalStage(),
                op.getOpName(), operators.keySet()));

        // initialize the operator's pushmsg client if needed
        for (Tm.OutputMetadata meta : request.getConfig().getOutputMetadataList()) {
            if (!pushMsgClients.containsKey(meta.getAddress())) {
                pushMsgClients.put(meta.getAddress(), new PushMsgClient(logger, meta.getAddress(), false));
            }
        }
        return op;
    }

    @Override
    public synchronized void addOperator(Tm.AddOperatorRequest request,
                                         StreamObserver<Empty> responseObserver) {
        logger.info("got add operator request"+request.getConfig());
        if (operators.size() >= operatorQuota) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator quota exceeded")));
            return;
        }
        logger.info("Display operators why key exists");
        for (String key : operators.keySet()) {
            logger.info(key);
        }
        logger.info("Finish");

        if (operators.containsKey(request.getConfig().getName())) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator already exists")));
            return;
        }
        logger.info(String.format("adding operator %d/%d", this.operators.size() + 1, this.operatorQuota));
        try {
            BaseOperator o = initOperator(request);
            kvProvider.addInvolvedOp(request.getConfig().getName(),o.hasKeySelector());
        } catch (IOException | ClassNotFoundException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("failed to initialize operator")));
            return;
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void pushMsgList(Tm.MsgList request, StreamObserver<Empty> responseObserver) {
        for (Tm.Msg msgElement : request.getMsgsList()) {
            String opName = msgElement.getReceiverOperatorName();
            //logger.info("got pushMsg request for "+opName);
            if(!operators.containsKey(opName)){
                responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator "+opName+" not found")));
                return;
            }
            try {
                opInputQueues.get(opName).put(msgElement);
            } catch (InterruptedException e) {
                String msg = String.format("interrupted while pushing message to %s", opName);
                logger.error(msg);
                responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(msg)));
                return;
            }
        }

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    public void handleStageMigration(List<Tm.OperatorConfig> newConfigs) {
        logger.info("handleStageMigration: {}", newConfigs);
        Map<String, Tm.OperatorConfig> cfgMap = new HashMap<>();
        for (Tm.OperatorConfig cfg : newConfigs) {
            cfgMap.put(cfg.getName(), cfg);
        }

        this.kvProvider.handleReconfig(Tm.ReconfigMsg.newBuilder().putAllConfig(cfgMap).build());
    }

    private boolean handleReconfigMsg(Tm.Msg msg) {
        // brute force look for source
        for(BaseOperator op : operators.values()){
            if(op instanceof SourceOperator){
                logger.info("got source op:{}",op.getOpName());
                // get the op's input queue
                LinkedBlockingQueue<Tm.Msg> inputQueue = opInputQueues.get(op.getOpName());
                assert inputQueue != null;
                // put the reconfig msg into the input queue
                try {
                    inputQueue.put(msg);
                    logger.info("Added to input queue {}", op.getOpName());
                } catch (InterruptedException e) {
                    logger.error("interrupted while pushing message to {}", op.getOpName());
                    return false;
                }
                return true;
            }
        }
        logger.warn("no source op found, see other tm");
        return false;
    }

    /**
     *
     */
    @Override
    public void removeOperator(Tm.RemoveOperatorRequest request,
                               StreamObserver<Empty> responseObserver) {
        // the operator's load should have been redirected by this point,
        // so we can just remove it from the operators map
        String opName = request.getOperatorName();
        BaseOperator op = operators.get(opName);
        if (op == null) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator " + opName + " not found")));
            return;
        }
        this.roundRobinCounter.remove(op);
        this.operators.remove(opName);
        this.opInputQueues.remove(opName);
        this.kvProvider.removeInvolvedOp(opName);
        logger.info(String.format("removed operator %s --all: %s", opName, operators.keySet()));
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getOperatorLowWatermark(Tm.OperatorLowWatermarkRequest request,
                                        StreamObserver<Tm.OperatorLowWatermarkResponse> responseObserver) {
        String opName = request.getName();
        String content = request.getContent();
        if(!operators.containsKey(opName)){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator " + opName + " not found")));
            return;
        }
        BaseOperator op = operators.get(opName);
        Tm.OperatorLowWatermarkResponse.Builder b = Tm.OperatorLowWatermarkResponse.newBuilder();
        logger.info("GET LOW WATERMARK: " + op.getMinOfMaxWatermark() + " for " + opName);
        b.setLowWatermark(op.getMinOfMaxWatermark());
        responseObserver.onNext(b.build());
        responseObserver.onCompleted();
    }

    @Override
    public void setOperatorExternalTimestamp(Tm.OperatorExternalTimestampRequest request,
                                             StreamObserver<Empty> responseObserver) {
        String opName = request.getName();
        long reconfigTimestamp = request.getReconfigTimestamp();
        if(!operators.containsKey(opName)){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator " + opName + " not found")));
            return;
        }
        BaseOperator op = operators.get(opName);
        logger.info("RECONFIG TIMESTAMP: " + reconfigTimestamp + " for "+ opName);
        op.setReconfigTimestamp(reconfigTimestamp);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getState(Tm.GetStateRequest request, StreamObserver<Tm.GetStateResponse> responseObserver) {
        String stateKey = request.getStateKey();
        // note that we're assuming that if a state is remote, then it must exist, thus could not be null anyways
        Object state = this.localKVProvider.get(stateKey, null);
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
    public void listStateKeys(Tm.ListKeysRequest request, StreamObserver<Tm.ListKeysResponse> responseObserver) {
        String prefix = request.getPrefix();
        List<String> keys = this.localKVProvider.listKeys(prefix);
        Tm.ListKeysResponse.Builder builder = Tm.ListKeysResponse.newBuilder();
        builder.addAllKeys(keys);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void pullStates(Tm.PullStatesRequest req, StreamObserver<Tm.PullStatesResponse> responseObserver){
        List<Tm.StateKV> result = new ArrayList<>();
        logger.info("pullStates: {}", req);
        for(String opName:operators.keySet()){
            if(operators.get(opName).getConfig().getLogicalStage()!=req.getLogicalStage()){
                continue;
            }
            System.out.println("StdPrefix: "+getStdStatePrefix(operators.get(opName)));
            List<String> keys = kvProvider.listKeys(getStdStatePrefix(operators.get(opName)));
            logger.info("got keys: {}", keys);
            for(String key:keys){
                int keyInt = KeyUtil.getHashFromKey(key);
                if(keyInt<req.getPartitionPlan().getPartitionStart() || keyInt>=req.getPartitionPlan().getPartitionEnd()){
                    continue;
                }
                Object state = kvProvider.get(key, null);
                ByteString obj = ByteString.copyFrom(BytesUtil.ObjectToBytes(state));
                result.add(Tm.StateKV.newBuilder().setKey(key).setObj(obj).build());
            }
        }
        Tm.PullStatesResponse response = Tm.PullStatesResponse.newBuilder().addAllStateKVs(result).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void removeState(Tm.RemoveStateRequest request, StreamObserver<Empty> responseObserver) {
        try {
            String stateKey = request.getStateKey();
            localKVProvider.delete(stateKey);
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
    public void updateState(Tm.UpdateStateRequest request, StreamObserver<Empty> responseObserver) {
        String stateKey = request.getStateKey();
        byte[] stateBytes = request.getObj().toByteArray();
        localKVProvider.put(stateKey, stateBytes);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }


    private void sendLoop() throws InterruptedException {
        while (true) {
            this.pendingOutputMsgs.clear();
            long current_startTime = System.currentTimeMillis();
            // operator-name, serialized msg, <partition key, msg type>
            if (msgQueue.size() < tmConfig.batch_size && System.currentTimeMillis() - current_startTime < tmConfig.batch_timeout_ms) {
                //logger.info("sendloop: msg queue size is " + msgQueue.size() + ", waiting for more messages, batch size is " + tmConfig.batch_size + " ...");
                Thread.sleep(10);
                continue;
            }
            int loopLength = Math.min(tmConfig.batch_size, msgQueue.size());
            List<OutputMessage> items = new ArrayList<>();
            for (int i = 0; i < loopLength; i++) {
                items.add(msgQueue.take());
            }

//            OutputMessage item = msgQueue.take();
//            Triple<String, Tm.Msg.Builder, Pair<Integer, Tm.Msg.MsgType>> item = msgQueue.take();
            //logger.info("sendloop: sending msg to "+item.getKey()+" with type "+item.getMsg().getType()

            for (OutputMessage item : items) {
                String opName = item.getOpName();
                BaseOperator op = operators.get(opName);
                Tm.OperatorConfig config = op.getConfig();
                List<Tm.OutputMetadata> targetOutput = new ArrayList<>();
                // apply the partition strategy
                if (item.getMsg().getType() == Tm.Msg.MsgType.WATERMARK) {
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
                            int key = item.getKey();

                            logger.info("{} got key: "+key, opName);
                            boolean ok = false;
                            for(Tm.OutputMetadata outputMetadata:config.getOutputMetadataList()){
                                if(
                                        outputMetadata.getPartitionPlan().getPartitionStart()<=key
                                                && outputMetadata.getPartitionPlan().getPartitionEnd()>=key
                                ){
                                    targetOutput.add(outputMetadata);
                                    ok = true;
                                    break;
                                }
                            }
                            if(!ok){
                                FatalUtil.fatal( "no output found for key "+key,null);
                            }
                            break;
                        case BROADCAST:
                            targetOutput = config.getOutputMetadataList();
                            break;
                    }
                }
                Tm.Msg.Builder msgBuilder = item.getMsg();
                for(Tm.OutputMetadata target: targetOutput){
                    Tm.Msg msg = msgBuilder.setReceiverOperatorName(target.getName()).build();
                    if (!pendingOutputMsgs.containsKey(target.getAddress())) {
                        pendingOutputMsgs.put(target.getAddress(), new ArrayList<>());
                    }
                    pendingOutputMsgs.get(target.getAddress()).add(msg);
//                    pushMsgClients.get(target.getAddress()).pushMsg(msg);
                }
//                logger.debug("sendloop: sending msg to" + targetOutput);
            }
            logger.debug("sendloop: sending batch of size " + items.size());
            for (String addr : pendingOutputMsgs.keySet()) {
                Tm.MsgList.Builder msgListBuilder = Tm.MsgList.newBuilder();
                Tm.MsgList msgList = msgListBuilder.addAllMsgs(pendingOutputMsgs.get(addr)).build();
                pushMsgClients.get(addr).pushMsgList(msgList);
            }
            current_startTime = System.currentTimeMillis();
        }
    }

    private String getStdStatePrefix(BaseOperator op){
        return op.getOpName() + ":" + op.getConfig().getLogicalStage() + ".";
    }

    @Override
    public ValueStateAccessor getValueStateAccessor(BaseOperator op, String stateName, Object defaultValue) {
        checkStateName(stateName);
        return new ValueStateAccessor(getStdStatePrefix(op)+stateName, this.kvProvider, defaultValue, op);
    }

    @Override
    public MapStateAccessor getMapStateAccessor(BaseOperator op, String stateName) {
        checkStateName(stateName);
        return new MapStateAccessor(getStdStatePrefix(op)+stateName,  this.kvProvider, op);
    }

    @Override
    public ListStateAccessor getListStateAccessor(BaseOperator op, String stateName) {
        checkStateName(stateName);
        return new ListStateAccessor(getStdStatePrefix(op)+stateName, this.kvProvider,op);
    }

    private void checkStateName(String name) {
        if (name.contains(".")) {
            throw new IllegalArgumentException("state name can not contain '.'");
        }
    }
}
