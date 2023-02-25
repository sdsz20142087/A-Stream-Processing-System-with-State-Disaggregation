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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import pb.OPServiceGrpc;

public abstract class BaseOperator extends Thread implements IOperator {
    private LinkedBlockingQueue<Op.Msg> inputQueue;
    private LinkedBlockingQueue<Op.Msg> outputQueue;
    private Logger logger = LogManager.getLogger();
    private List<OPServiceGrpc.OPServiceStub> stubs;
    private Op.OperatorConfig config;
    private Server server; // grpc server for receiving input from upstream operators

    public BaseOperator(Op.OperatorConfig config) {
        this.inputQueue = new LinkedBlockingQueue<>();
        this.outputQueue = new LinkedBlockingQueue<>();
        this.config = config;
        initNextOpClients();
        startGRPCServer();
    }

    // external interface for adding input by TM(task manager)
    public void addInput(Op.Msg input) {
        inputQueue.add(input);
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
    }

    private void startGRPCServer() {
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
        // start another new thread that sends output to next operators
        Thread sender = new Thread(() -> {
            try {
                while (true) {
                    // send output to next operators
                    sendWithStrategy();
                }
            } catch (InterruptedException e) {
                logger.fatal("Exception in sender thread: " + e.getMessage());
                System.exit(1);
            }
        });
        sender.start();


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
        switch (config.getPartitionStrategy()) {
            case ROUND_ROBIN:
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
            case HASH:
                break;
            case BROADCAST:
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
    }

}
