package controller;

import config.CPConfig;
import config.Config;
import kotlin.Pair;
import kotlin.Triple;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Tm;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class Scheduler extends Thread{

    private QueryPlan plan;

    private CPConfig cpcfg;

    private double scale_up_portion;
    private HashMap<String, TMClient> tmClients;

    private OperatorLoadBalancer opLB;

    Comparator<Triple<Integer, String, TMClient>> comparator = new Comparator<Triple<Integer, String, TMClient>>() {
        @Override
        public int compare(Triple<Integer, String, TMClient> t1, Triple<Integer, String, TMClient> t2) {
            int cmp = Integer.compare(t1.getFirst(), t2.getFirst());
            return cmp;
        }
    };
    private PriorityBlockingQueue<Triple<Integer, String, TMClient>> pq = new PriorityBlockingQueue<>((Collection) comparator);

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

    private HashMap<Pair<String, String>, Tm.OperatorConfig.Builder> operators_config;
    private transient LinkedBlockingQueue<Triple<String, String, Integer>> inputQueue;
    private transient LinkedBlockingQueue<Triple<String, String, Integer>> outputQueue;

    private Logger logger = LogManager.getLogger();

    public Scheduler(QueryPlan plan, HashMap<String, TMClient> tmClients){
        this.plan = plan;
        this.tmClients = tmClients;
        this.cpcfg = Config.getInstance().controlPlane;
        this.scale_up_portion = this.cpcfg.scale_up_portion;
        this.opLB = ControlPlane.getInstance().opLB;
        // TODO: analyze the existing planConfig and assign operators to TM
    }

    public void scaleTMOperator(String tm_name, String op_name, int inputQueueLength) {
        inputQueue.add(new Triple<>(tm_name, op_name, inputQueueLength));
    }

    @Override
    public void run() {
        // TODO: deploy the query plan to TMs
        // FIXME: naive implementation
        int i=0;
        List<QueryPlan.OperatorInfo> flatList = plan.getFlatList();
        int stageDepth = plan.getStages().size();
        HashMap<Integer, List<Tm.OutputMetadata>> stageToTm = new HashMap<>();
        for(int j=0; j<stageDepth; j++){
            stageToTm.put(j, new ArrayList<>());
        }
        while(i<flatList.size()){
            // read the clients on the fly since cluster membership may change
//            Collection<TMClient> clients = tmClients.values();//fixme: use later
            for (String key : tmClients.keySet()) {
                TMClient client = tmClients.get(key);
                pq.offer(new Triple<Integer, String, TMClient>(0, key, client));
            }
            // TODO: assign operator config's outputAddress and bufferSize
            while(i<flatList.size()) {
                // !! deploy the operators in reverse order, so we know the outputAddress of the next stage
                QueryPlan.OperatorInfo item = flatList.get(flatList.size() - 1 - i);
                Tm.OperatorConfig.Builder cfgBuilder = null;
                int parallelism = item.getParallelism();
                while (parallelism > 0) {
                    Triple<Integer, String, TMClient> poll_element = pq.poll();
                    TMClient tmClient = poll_element.getThird();
                    opLB.registerOperator(item.getStageIdx(), stageDepth - 1, item.getCfg(), item.getOp(), tmClient);
                    parallelism--;
                }
                if (item.getStageIdx() != stageDepth - 1) {
                    // assign the outputAddress and bufferSize
                    // FIXME: in cases like keyby, the stream is not fully connected between stages, extra impl is needed
                    cfgBuilder = item.getCfg();
                    cfgBuilder.addAllOutputMetadata(stageToTm.get(item.getFirst() + 1));
                }
                operators_config.put(new Pair<>(poll_element.getSecond(), cfgBuilder.getName()), cfgBuilder);
                try {
                    tmClient.addOperator(item.getSecond().build(), item.getThird());
                    poll_element = new Triple<>(poll_element.getFirst() + 1, poll_element.getSecond(), poll_element.getThird());
                    pq.offer(poll_element);
                    Tm.OutputMetadata meta = Tm.OutputMetadata.
                            newBuilder().
                            setAddress(tmClient.getAddress()).
                            setName(item.getSecond().getName()).
                            build();
                    stageToTm.get(item.getFirst()).add(meta);
                    i++;
                } catch (Exception e) {
                    logger.error("Failed to add operator to TM at " + tmClient.getHost() + tmClient.getPort(), e);
                    break;
                }
            }
            if(i<flatList.size()){
                logger.warn("Did not sucessfully deploy all operators to TMs. Retrying in 2 seconds...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }
        }


        // TODO: check the status and see if scaling is needed
        while(true){
            try {
                logger.info("CP: check status");
                Triple<String, String, Integer> op_request = inputQueue.take();
                Tm.OperatorConfig.Builder op_config = operators_config.get(new Pair<>(op_request.getFirst(), op_request.getSecond()));
                int operator_threshold = (int) (op_config.getBufferSize() * scale_up_portion);
                if (!(operator_threshold >= op_request.getThird())) {
                    logger.info("processing scale up");

                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}