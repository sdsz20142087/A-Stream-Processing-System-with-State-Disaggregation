package operators;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Op;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import pb.OPServiceGrpc;

public abstract class BaseOperator extends Thread implements IOperator, Serializable {
    private LinkedBlockingQueue<Op.Msg> inputQueue;
    private LinkedBlockingQueue<Op.Msg> outputQueue;
    protected Logger logger = LogManager.getLogger();
    private List<OPServiceGrpc.OPServiceStub> stubs;
    private Op.OperatorConfig config;
    private Server server; // grpc server for receiving input from upstream operators
    private int bufferSize = 1000; // UDF buffer size, can change in runtime
    private static int paritionID = 0; // use for Round Robin

    // There must not be moving parts (e.g. listening to ports, starting new threads)
    // in the constructor because we'll be sending this object over grpc.
    public BaseOperator(Op.OperatorConfig config) {
        this.inputQueue = new LinkedBlockingQueue<>();
        this.outputQueue = new LinkedBlockingQueue<>();
        this.config = config;
        this.setName(config.getName());
    }

    public Op.OperatorConfig getConfig(){
        return this.config;
    }

    // external interface for adding input by TM(task manager)
    public void addInput(Op.Msg input) {
        inputQueue.add(input);
        logger.debug(String.format("OP: input: size=%d", inputQueue.size()));
    }

    protected void sendOutput(Op.Msg output) {
        outputQueue.add(output);
    }

    private void handleMsg(Op.Msg input) {
        switch (input.getType()) {
            case DATA:
                processElement(input.getData());
                break;
            case CONTROL:
                // do something about the control msg
                handleConfigUpdate(input.getConfig());
                logger.info("got control msg: " + input);
                // send it downstream
                sendOutput(input);
                break;
        }
    }

    // emitting output is done in the processElement method
    protected abstract void processElement(ByteString in);

    private void initNextOpClients() {
        stubs = new ArrayList<>();
        for (String addr : config.getNextOperatorAddressList()) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(addr).usePlaintext().build();
            stubs.add(OPServiceGrpc.newStub(channel));
        }
        logger.info("OP: NextOperatorAddr:"+config.getNextOperatorAddressList());
    }

    protected void startGRPCServer() {
        // start a new thread that receives input from upstream operators
        if (this.server != null && !this.server.isShutdown()) {
            this.server.shutdownNow();
        }
        this.server = ServerBuilder.forPort(this.config.getListenPort())
                .addService(new OpServiceImpl(this))
                .build();
        try{
            this.server.start();
        } catch (Exception e) {
            logger.fatal("Exception in starting grpc server: " + e.getMessage());
            System.exit(1);
        }
        logger.info("Operator " + config.getName() + " started grpc server on port " + config.getListenPort());
    }

    @Override
    public void run() {
        initNextOpClients();
        startGRPCServer();

        // start another new thread that sends output to next operators
        Thread sender = new Thread(() -> {
            try {
                int currentPartitionId;
                if(config.getPartitionStrategy()==Op.PartitionStrategy.ROUND_ROBIN){
                    currentPartitionId= 0;
                }
                while (true) {
                    // send output to next operators with the specified strategy
                    sendWithStrategy();
                }
            } catch (InterruptedException e) {
                logger.fatal("Exception in sender thread: " + e.getMessage());
                System.exit(1);
            }
        });

        sender.start();

        // receive input from upstream operators
        try {
            while (true) {
                Op.Msg input = inputQueue.take();
                handleMsg(input);
            }
        } catch (Exception e) {
            logger.fatal("Exception in sender thread: " + e.getMessage());
            System.exit(1);
        }
        logger.info("Operator " + config.getName() + " started");
    }

    private void sendWithStrategy() throws InterruptedException {
        Op.Msg msg = outputQueue.take();
        OPServiceGrpc.OPServiceStub targetStub;
        switch (config.getPartitionStrategy()) {
            case ROUND_ROBIN:
                targetStub= stubs.get(paritionID);
                targetStub.pushMsg(msg, new StreamObserver<>() {
                    @Override
                    public void onNext(Empty e) {
                        // do nothing
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("Error in sending output to next operator: " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        // do nothing
                    }
                });
                paritionID = (paritionID + 1) % stubs.size();
                break;
            case HASH:
                targetStub= stubs.get(msg.getPartitionId());
                targetStub.pushMsg(msg, new StreamObserver<>() {
                    @Override
                    public void onNext(Empty e) {
                        // do nothing
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("Error in sending output to next operator: " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        // do nothing
                    }
                });
                break;
            case BROADCAST:
                for (OPServiceGrpc.OPServiceStub stub : stubs) {
                    stub.pushMsg(msg, new StreamObserver<>() {
                        @Override
                        public void onNext(Empty e) {
                            // do nothing
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.error("Error in sending output to next operator: " + throwable.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            // do nothing
                        }
                    });
                }
                break;
            case RANDOM:
                break;
            default:
                break;
        }
    }

    public synchronized void handleConfigUpdate(Op.OperatorConfig config) {
        Op.OperatorConfig oldConfig = this.config;
        this.config = config;
        // TODO: other types of re-init
        if(oldConfig.getListenPort()!=config.getListenPort()){
            this.startGRPCServer();
        }
        if(oldConfig.getNextOperatorAddressList().equals(config.getNextOperatorAddressList())){
            this.initNextOpClients();
        }
        if(oldConfig.getBufferSize()!=config.getBufferSize()) {
            this.bufferSize = config.getBufferSize();
        }
    }
    public boolean checkBuffer(){
        return inputQueue.size() > bufferSize;
    }

}
