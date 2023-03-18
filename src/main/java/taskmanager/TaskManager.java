package taskmanager;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import config.Config;
import config.TMConfig;
import exec.SerDe;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import operators.BaseOperator;
import operators.ISource;
import operators.SinkOperator;
import operators.SourceOperator;
import pb.Op;
import pb.Tm;
import utils.NodeBase;
import utils.StringSerde;
import utils.WikiFileSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;

public class TaskManager extends NodeBase {
    private static TaskManager instance;
    private final RegistryClient registryClient;
    private final Server tmServer;
    private final TMConfig tmcfg;
    private final TMServiceImpl tmService;

    private TaskManager(int grpcPort) {

        // read the config file
        Config.LoadConfig(configPath);

        tmcfg = Config.getInstance().taskManager;
        tmcfg.tm_port = grpcPort;
        logger.info("tm_port=" + Config.getInstance().taskManager.tm_port);
        registryClient = new RegistryClient(tmcfg.cp_host, tmcfg.cp_port, tmcfg.tm_port);
        tmService = new TMServiceImpl(tmcfg.operator_quota);
        tmServer = ServerBuilder.forPort(tmcfg.tm_port).addService(tmService).build();
    }

    public void start() {
        try {
            // register at control plane
            logger.info("Registering at control plane=" + tmcfg.cp_host + ":" + tmcfg.cp_port);
            logger.info(getHost());
            registryClient.registerTM(getHost(), getName());
            this.tmServer.start();
            logger.info("TaskManager started on " + tmcfg.tm_port);
            // let this thread block until server termination

            // simulate control plane requests
            // ------------------------------------------
            Thread.sleep(1000);
            logger.info("starting ops simulation");
            ArrayList<String> addrs = new ArrayList<>();
            addrs.add("127.0.0.1:9002");
            Op.OperatorConfig cfgSink = Op.OperatorConfig.newBuilder()
                    .setName("sink-1-test")
                    .setType(Op.OperatorType.SINK)
                    .setListenPort(9002)
                    .setPartitionStrategy(Op.PartitionStrategy.ROUND_ROBIN)
                    .build();
            Op.OperatorConfig cfgSource = Op.OperatorConfig.newBuilder()
                    .setName("source-1-test")
                    .setType(Op.OperatorType.SOURCE)
                    .setListenPort(9000)
                    .addAllNextOperatorAddress(addrs)
                    .setPartitionStrategy(Op.PartitionStrategy.ROUND_ROBIN)
                    .build();
            ISource<String> src = new WikiFileSource("data.txt");
            SerDe<String> serde = new StringSerde();
            SourceOperator<String> sourceOperator = new SourceOperator<String>(cfgSource, src, serde);
            SinkOperator sink = new SinkOperator(cfgSink);

            LinkedList<BaseOperator> ops = new LinkedList<>();
            ops.add(sink);
            ops.add(sourceOperator);
            for(BaseOperator op : ops) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(op);
                byte[] bytes = baos.toByteArray();
                ByteString bs = ByteString.copyFrom(bytes);
                logger.info("got operator "+op.getName()+" bytes, size=" + bytes.length + "");

                Tm.AddOperatorRequest req = Tm.AddOperatorRequest
                        .newBuilder()
                        .setConfig(cfgSource)
                        .setObj(bs)
                        .build();
                tmService.addOperator(req, new StreamObserver<Empty>() {
                    @Override
                    public void onNext(Empty empty) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("add operator error", throwable);
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("add operator completed");
                    }
                });
                logger.info("sleeping 2");
                Thread.sleep(2000);
                logger.info("woke up");
            }
            // ------------------------------------------

            this.tmServer.awaitTermination();
        } catch (IOException | InterruptedException e) {
            logger.fatal("Failed to start TaskManager", e);
            System.exit(1);
        }
    }

    public static TaskManager getInstance() {
        if (instance == null)
            instance = new TaskManager(8010);
        return instance;
    }

    public static void main(String[] args) {
        TaskManager.getInstance().start();
    }
}
