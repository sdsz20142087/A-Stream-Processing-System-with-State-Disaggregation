package controller;

import config.CPConfig;
import config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Cp;
import pb.Tm;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class Scheduler extends Thread {

    private QueryPlan plan;

    private CPConfig cpcfg;

    private double scale_up_portion;
    private HashMap<String, TMClient> tmClients;

    private OperatorLoadBalancer opLB;
    static class Triple<F, S, T> {
        private F first;
        private S second;
        private T third;

        public Triple(F first, S second, T third) {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        public F getFirst() {
            return first;
        }

        public S getSecond() {
            return second;
        }

        public T getThird() {
            return third;
        }

    }

    Comparator<Triple<Integer, String, TMClient>> comparator = new Comparator<Triple<Integer, String, TMClient>>() {
        @Override
        public int compare(Triple<Integer, String, TMClient> t1, Triple<Integer, String, TMClient> t2) {
            return t1.getFirst().compareTo(t2.getFirst());
        }
    };
    /**
     * priority_queue to choose appropriate tmClient, based on the operator number this tmClient responsible for.
     * triple(operatorCnt, tmClientName, tmClient)
     */
    private PriorityBlockingQueue<Triple<Integer, String, TMClient>> pq = new PriorityBlockingQueue<>(20, comparator);

    static class Pair<A, B> {
        private final A first;
        private final B second;

        public Pair(A first, B second) {
            this.first = first;
            this.second = second;
        }

        public A getFirst() {
            return first;
        }

        public B getSecond() {
            return second;
        }

        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + (first == null ? 0 : first.hashCode());
            result = 31 * result + (second == null ? 0 : second.hashCode());
            return result;
        }
    }

    /**
     * key: pair(tmClientName, opCompletedName), value: operatorConfig
     */
    private HashMap<Pair<String, String>, Tm.OperatorConfig.Builder> operators_config = new HashMap<>();
    //handle ReportQueueStatusRequest
    private transient LinkedBlockingQueue<Triple<String, String, Integer>> inputQueue = new LinkedBlockingQueue<>();
    private transient LinkedBlockingQueue<Triple<String, String, Integer>> outputQueue = new LinkedBlockingQueue<>();

    // auto-increment

    private Logger logger = LogManager.getLogger();

    public Scheduler(QueryPlan plan, HashMap<String, TMClient> tmClients) {
        this.plan = plan;
        this.tmClients = tmClients;
        this.cpcfg = Config.getInstance().controlPlane;
        this.scale_up_portion = this.cpcfg.scale_up_portion;
    }

    public void scaleTMOperator(String tm_name, String op_name, int inputQueueLength) {
        inputQueue.add(new Triple<>(tm_name, op_name, inputQueueLength));
    }

    @Override
    public void run() {
        this.opLB = ControlPlane.getInstance().opLB;
        int i = 0;
        List<QueryPlan.OperatorInfo> flatList = plan.getFlatList();
        int stageDepth = plan.getStages().size();
        int totalOperator = 0;
        int singleOperatorCnt = 0;
        for (int k = 0; k < flatList.size(); k++) {
            totalOperator += flatList.get(k).getParallelism();
        }
        // read the clients on the fly since cluster membership may change
//            Collection<TMClient> clients = tmClients.values();//fixme: use later
        for (String key : tmClients.keySet()) {
            TMClient client = tmClients.get(key);
            pq.offer(new Triple<Integer, String, TMClient>(0, key, client));
        }
        while (i < flatList.size()) {
            // !! deploy the operators in reverse order, so we know the outputAddress of the next stage
            QueryPlan.OperatorInfo item = flatList.get(flatList.size() - 1 - i);
            int parallelism = item.getParallelism();
            int paraIdx = parallelism;
            while (paraIdx > 0) {
                singleOperatorCnt++;
                Triple<Integer, String, TMClient> poll_element = pq.poll();
                assert poll_element != null;
                TMClient tmClient = poll_element.getThird(); //choose appropriate tmClient
                Tm.OperatorConfig.Builder parallelConfig = Tm.OperatorConfig.newBuilder()
                        .setLogicalStage(item.getCfg().getLogicalStage())
                        .setName(item.getCfg().getName())
                        .setPartitionStrategy(item.getCfg().getPartitionStrategy())
                        .setBufferSize(item.getCfg().getBufferSize())
                        .addAllOutputMetadata(item.getCfg().getOutputMetadataList());
                Tm.OperatorConfig.Builder completedOpConfig = opLB.registerOperator(item.getStageIdx(), stageDepth - 1, parallelConfig, item.getOp(), tmClient, parallelism - paraIdx);
                operators_config.put(new Pair<>(poll_element.getSecond(), completedOpConfig.getName()), completedOpConfig);
                paraIdx--;
                poll_element = new Triple<>(poll_element.getFirst() + 1, poll_element.getSecond(), poll_element.getThird());
                pq.offer(poll_element);
                logger.info("load " + singleOperatorCnt + "/" + totalOperator);
            }
            i++;
        }
        // TODO: check the status and see if scaling is needed
        while (true) {
            try {
                logger.info("CP: check status");
                //tmClientName, opCompletedName, inputQueueLength
                Triple<String, String, Integer> op_request = inputQueue.take();
                Tm.OperatorConfig.Builder op_config = operators_config.get(new Pair<>(op_request.getFirst(), op_request.getSecond()));
                int operator_threshold = (int) (op_config.getBufferSize() * scale_up_portion);
                if (!(operator_threshold >= op_request.getThird())) {
                    logger.info("processing scale up");
                    Triple<Integer, String, TMClient> target_element = pq.poll();
                    TMClient tmClient = target_element.getThird();
                    Tm.OperatorConfig.Builder scaledOpConfig = opLB.scaleUpOp(op_config, tmClient);
                    operators_config.put(new Pair<>(target_element.getSecond(), scaledOpConfig.getName()), scaledOpConfig);
                    target_element = new Triple<>(target_element.getFirst() + 1, target_element.getSecond(), target_element.getThird());
                    pq.offer(target_element);

                    logger.info("scale up successfully, new scaled op is " + scaledOpConfig.getName());
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}