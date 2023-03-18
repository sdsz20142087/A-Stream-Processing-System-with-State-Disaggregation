package taskmanager;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import kotlin.Pair;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.TMServiceGrpc;
import pb.Tm;
import utils.TMException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

class TMServiceImpl extends TMServiceGrpc.TMServiceImplBase {
    private final int operatorQuota;
    private final HashMap<String, BaseOperator> operators;
    private final Logger logger = LogManager.getLogger();

    // map of< TM's address, PushMsgClient>
    private final Map<String, PushMsgClient> pushMsgClients = new HashMap<>();

    private final Map<String, LinkedBlockingQueue<Tm.Msg>> opInputQueues = new HashMap<>();

    // map of <operator name, message>
    private final LinkedBlockingQueue<Pair<String, Tm.Msg>> msgQueue = new LinkedBlockingQueue<>();

    public TMServiceImpl(int operatorQuota) {
        super();
        this.operatorQuota = operatorQuota;
        operators = new HashMap<>();
        logger.info("TM service started with operator quota: " + operatorQuota);
        // boot the send loop
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
        this.opInputQueues.put(op.getName(), inputQueue);
        // the queues must be initialized before the operator starts
        op.init(inputQueue, msgQueue);
        op.start();
        logger.info(String.format("Started operator %s", op.getName()));
        operators.put(op.getName(), op);

        // initialize the operator's pushmsg client if needed
        for(String addr: request.getConfig().getOutputAddressList()){
            if(!pushMsgClients.containsKey(addr)){
                pushMsgClients.put(addr, new PushMsgClient(logger, addr));
            }
        }
    }

    @Override
    public synchronized void addOperator(Tm.AddOperatorRequest request,
                            StreamObserver<Empty> responseObserver) {
        if (operators.size() >= operatorQuota) {
            responseObserver.onError(new TMException("operator quota exceeded"));
            return;
        }
        if (operators.containsKey(request.getConfig().getName())) {
            responseObserver.onError(new TMException("operator name already exists"));
            return;
        }
        logger.info(String.format("adding operator %d/%d",this.operators.size()+1,this.operatorQuota));
        try{
            initOperator(request);
        } catch (IOException | ClassNotFoundException e) {
            responseObserver.onError(new TMException("failed to deserialize operator"));
            return;
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void pushMsg(Tm.Msg request, StreamObserver<Empty> responseObserver) {
        logger.info("got pushMsg request");
        String opName = request.getOperatorName();
        if(!operators.containsKey(opName)){
            responseObserver.onError(new TMException("operator not found"));
            return;
        }
        try {
            opInputQueues.get(opName).put(request);
        } catch (InterruptedException e) {
            responseObserver.onError(new TMException("failed to push message to operator"));
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

    }

    private void sendLoop() throws InterruptedException {
        while (true) {
            Pair<String, Tm.Msg> msg = msgQueue.take();
            String opName = msg.getFirst();
            Tm.OperatorConfig config = operators.get(opName).getConfig();
            List<String> targetAddr = new ArrayList<>();
            // apply the partition strategy
            switch (config.getPartitionStrategy()) {
                case ROUND_ROBIN:
                    targetAddr.add(config.getOutputAddressList().get(0));
                    break;
                case HASH:
                    // ???

                    // FIXME: WHAT DOES THIS EVEN MEAN? targetStub= stubs.get(msg.getPartitionId());
                    break;
                case BROADCAST:
                    targetAddr = config.getOutputAddressList();
                    break;
                case RANDOM:
                    // ???
                    break;
            }
            for(String addr: targetAddr){
                pushMsgClients.get(addr).pushMsg(msg.getSecond());
            }
        }
    }
}