package taskmanager;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import kotlin.Pair;
import operators.BaseOperator;
import operators.StateDescriptorProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;
import stateapis.ListStateAccessor;
import stateapis.MapStateAccessor;
import stateapis.ValueStateAccessor;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

class TMServiceImpl extends TMServiceGrpc.TMServiceImplBase implements StateDescriptorProvider {
    private final int operatorQuota;
    private final HashMap<String, BaseOperator> operators;
    private HashMap<String, BaseState> states;
    private final Logger logger = LogManager.getLogger();

    // map of< TM's address, PushMsgClient>
    private final Map<String, PushMsgClient> pushMsgClients = new HashMap<>();

    private final Map<String, LinkedBlockingQueue<Tm.Msg>> opInputQueues = new HashMap<>();

    // map of <operator name, message>
    private final LinkedBlockingQueue<Pair<String, Tm.Msg.Builder>> msgQueue = new LinkedBlockingQueue<>();

    public TMServiceImpl(int operatorQuota) {
        super();
        this.operatorQuota = operatorQuota;
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
        b.setOperatorCount(999);
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
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois;

        ois = new ObjectInputStream(bis);
        BaseOperator op = (BaseOperator) ois.readObject();
        LinkedBlockingQueue<Tm.Msg> inputQueue = new LinkedBlockingQueue<>();
        // the queues must be initialized before the operator starts
        op.init(request.getConfig(), inputQueue, msgQueue, this);
        op.start();
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
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("operator not found")));
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
        BaseState state= states.get(stateKey);
        ByteString stateBytes = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(state);
            oos.flush();
            stateBytes = ByteString.copyFrom(baos.toByteArray());
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
        if (!states.containsKey(request.getStateKey())){
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription("state not found")));
            return;
        }
        try {
            String stateKey = request.getStateKey();
            states.remove(stateKey);
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
    public void updateState(Tm.UpdateStateRequest request, StreamObserver <Empty> responseObserver){
        String stateKey = request.getStateKey();
        byte[] stateBytes = request.getObj().toByteArray();
        BaseState state = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(stateBytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            state = (BaseState) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription("Failed to deserialize state object")));
            return;
        }
        states.replace(stateKey, state);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();

    }


    private void sendLoop() throws InterruptedException {
        while (true) {
            Pair<String, Tm.Msg.Builder> item = msgQueue.take();
            String opName = item.getFirst();
            Tm.OperatorConfig config = operators.get(opName).getConfig();
            List<Tm.OutputMetadata> targetOutput = new ArrayList<>();
            // apply the partition strategy
            switch (config.getPartitionStrategy()) {
                case ROUND_ROBIN:
                    // FIXME: this is not correct
                    targetOutput.add(config.getOutputMetadataList().get(0));
                    break;
                case HASH:
                    // ???

                    // FIXME: WHAT DOES THIS EVEN MEAN? targetStub= stubs.get(msg.getPartitionId());
                    break;
                case BROADCAST:
                    targetOutput = config.getOutputMetadataList();
                    break;
                case RANDOM:
                    // ???
                    break;
            }
            for(Tm.OutputMetadata target: targetOutput){
                Tm.Msg msg = item.getSecond().setOperatorName(target.getName()).build();
                pushMsgClients.get(target.getAddress()).pushMsg(msg);
            }
            logger.info("sendloop: sending msg to"+targetOutput);
        }
    }

    // TODO: IMPLEMENT THIS
    @Override
    public ValueStateAccessor getValueStateAccessor(BaseOperator op, String stateName) {
        return null;
    }

    @Override
    public MapStateAccessor getMapStateAccessor(BaseOperator op, String stateName) {
        return null;
    }

    @Override
    public ListStateAccessor getListStateAccessor(BaseOperator op, String stateName) {
        return null;
    }
}