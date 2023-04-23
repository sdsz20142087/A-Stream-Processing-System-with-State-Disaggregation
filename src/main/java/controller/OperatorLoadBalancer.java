package controller;

import config.CPConfig;
import config.Config;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Tm;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class OperatorLoadBalancer{
    private static OperatorLoadBalancer instance;
    public static OperatorLoadBalancer getInstance(QueryPlan plan) {
        if (instance == null)
            instance = new OperatorLoadBalancer(plan);
        return instance;
    }

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

    static class OperatorTaskStatus {
        public OperatorTaskStatus(int pendingTaskCnt, int upstreamOperatorCnt, String opName, String tmAddr) {
            PendingTaskCnt = pendingTaskCnt;
            this.upstreamOperatorCnt = upstreamOperatorCnt;
            this.opName = opName;
            this.tmAddr = tmAddr;
        }

        public int getPendingTaskCnt() {
            return PendingTaskCnt;
        }

        public void setPendingTaskCnt(int pendingTaskCnt) {
            PendingTaskCnt = pendingTaskCnt;
        }

        public int getUpstreamOperatorCnt() {
            return upstreamOperatorCnt;
        }

        public void setUpstreamOperatorCnt(int upstreamOperatorCnt) {
            this.upstreamOperatorCnt = upstreamOperatorCnt;
        }

        public String getOpName() {
            return opName;
        }

        public void setOpName(String opName) {
            this.opName = opName;
        }

        public String getTmAddr() {
            return tmAddr;
        }

        public void setTmAddr(String tmAddr) {
            this.tmAddr = tmAddr;
        }

        int PendingTaskCnt;
        int upstreamOperatorCnt;
        String opName;
        String tmAddr;

    }

    Comparator<OperatorTaskStatus> comparator = new Comparator<OperatorTaskStatus>() {

        @Override
        public int compare(OperatorTaskStatus o1, OperatorTaskStatus o2) {
            int cmp = Integer.compare(o1.getPendingTaskCnt(), o2.getPendingTaskCnt());
            if (cmp == 0) {
                cmp = Integer.compare(o1.getUpstreamOperatorCnt(), o2.getUpstreamOperatorCnt());
            }
            return cmp;
        }
    };
    /**
     * @operators_distribution: key - opGeneralName, value - PQ(OperatorTaskStatus), order based on pendingTaskCnt and upstreamOperatorCnt
     */
    private ConcurrentHashMap<String, List<OperatorTaskStatus>> operators_distribution = new ConcurrentHashMap<>();
    /**
     * @stageToTm: key - stage idx, value - List(opGeneralName)
     */
    private ConcurrentHashMap<Integer, List<Tm.OutputMetadata>> stageToTm = new ConcurrentHashMap<>();
    /**
     * @upstream_operators: key - currentOpName, value - List(upStreamOp Config)
     */
    private ConcurrentHashMap<String, List<Tm.OperatorConfig.Builder>> upstream_operators = new ConcurrentHashMap<>();
    /**
     * @routeTable: key - opRealName, value - tmClient address
     */
    private ConcurrentHashMap<String, String> routeTable = new ConcurrentHashMap<>();
    /**
     * @op_configs: key - opRealName, value - opConfig
     */
    private ConcurrentHashMap<String, Tm.OperatorConfig.Builder> op_configs = new ConcurrentHashMap<>();
    /**
     * @hmBaseOperators: key - opGeneralName, value - baseOperator
     */
    private ConcurrentHashMap<String, BaseOperator> hmBaseOperators = new ConcurrentHashMap<>();
    private QueryPlan plan;
    private final int stageDepth;
    private CPConfig cpcfg;
    private double scale_up_portion;
    private List<QueryPlan.OperatorInfo> flatList;
    private final Logger logger = LogManager.getLogger();

    private OperatorLoadBalancer(QueryPlan plan) {
        this.plan = plan;
        this.flatList = plan.getFlatList();
        this.stageDepth = plan.getStages().size();
        this.cpcfg = Config.getInstance().controlPlane;
        this.scale_up_portion = this.cpcfg.scale_up_portion;
        for(int j=0; j<stageDepth; j++){
            stageToTm.put(j, new ArrayList<>());
        }
    }

    public Tm.OperatorConfig.Builder registerOperator(int stageIdx, int stageMaxDepth, Tm.OperatorConfig.Builder cfg, BaseOperator op, TMClient tmClient, int ParaCnt) {
        //For operators not in the last stage and don't write specific downStream Operators, add all next stage operators to its downstream op
        if (stageIdx != stageDepth - 1 && cfg.getOutputMetadataCount() == 0) {
            cfg.addAllOutputMetadata(stageToTm.get(stageIdx + 1));
        }
        //get real downStream operator, at first step the downStream Op is a general name, for example SourceOp_0,
        // however, for Parallelism = 3, realOpName are (SourceOp_0-0, SourceOp_0-1, SourceOp_0-2), so need to choose an appropriate op instance
        int n = cfg.getOutputMetadataCount();
        for (int i = 0; i < n; i++) {
            Tm.OutputMetadata completedMeta = cfg.getOutputMetadata(i);
            List<OperatorTaskStatus> realDownStreamOps = operators_distribution.get(completedMeta.getName()); //get op who has minimum load
            for (int j = 0; j < realDownStreamOps.size(); j++) {
                OperatorTaskStatus realDownStreamOp = realDownStreamOps.get(j);
                Tm.OutputMetadata newMeta = Tm.OutputMetadata.
                        newBuilder().
                        setName(realDownStreamOp.getOpName()).
                        setAddress(realDownStreamOp.getTmAddr()).
                        build();
                cfg.addOutputMetadata(newMeta);
                realDownStreamOp.upstreamOperatorCnt++;
                if (!upstream_operators.containsKey(realDownStreamOp.getOpName())) {
                    upstream_operators.put(realDownStreamOp.getOpName(), new ArrayList<>());
                }
                upstream_operators.get(realDownStreamOp.getOpName()).add(cfg); // update upStreamOp
            }
            cfg.removeOutputMetadata(i);
        }

        try {
            String op_name = cfg.getName(); //generalName
            if (!operators_distribution.containsKey(op_name)) {
                operators_distribution.put(op_name, new ArrayList<>());
            }
            List<OperatorTaskStatus> op_instances = operators_distribution.get(op_name);
            int instance_size = op_instances.size();
            cfg.setName(cfg.getName() + "-" + instance_size); //rename operator based on current instance size
            if (ParaCnt == 0) {
                Tm.OutputMetadata meta = Tm.OutputMetadata. // save generalName into stageToTm to prepare for following operator choice.
                        newBuilder().
                        setName(op_name).
                        build();
                stageToTm.get(stageIdx).add(meta);
                hmBaseOperators.put(op_name, op);
            }
            routeTable.put(cfg.getName(), tmClient.getAddress());
            OperatorTaskStatus opTS = new OperatorTaskStatus(0, 0, cfg.getName(), tmClient.getAddress());
            operators_distribution.get(op_name).add(opTS);
            logger.info("register operator: " + cfg.getName());
            tmClient.addOperator(cfg.build(), op);
            outputDownStreamOpInfo(cfg, tmClient.getAddress());
            op_configs.put(cfg.getName(), cfg);
        } catch (Exception e) {
            logger.error("Failed to add operator to TM at " + tmClient.getHost() + tmClient.getPort(), e);
        }
        return cfg;
    }

    public void outputDownStreamOpInfo(Tm.OperatorConfig.Builder cfg, String tmClientAddr) {
        logger.info(cfg.getName() + ": " + tmClientAddr);
        logger.info("DownStream Operators: " + cfg.getOutputMetadataCount());
        for (int i = 0; i < cfg.getOutputMetadataCount(); i++) {
            Tm.OutputMetadata outputMetadata = cfg.getOutputMetadata(i);
            logger.info("       -->" + outputMetadata.getName() + ": " + outputMetadata.getAddress());
        }
    }

    @Deprecated
    public boolean reRouteOperator(String tm_name, String op_name) {
//        String generalOpName = op_name.substring(0, op_name.length() - 2);
//        PriorityBlockingQueue<OperatorTaskStatus> operatorTaskStatuses = operators_distribution.get(generalOpName);
//        PriorityBlockingQueue<OperatorTaskStatus> newPQ = updatePQTaskStatus(operatorTaskStatuses); //get latest pendingTaskLength for each
//        operators_distribution.put(generalOpName, newPQ);
//        OperatorTaskStatus taskStatus = newPQ.poll(); //target re-Route operator, who has the least pendingTaskLength
//        Tm.OperatorConfig.Builder cfg = op_configs.get(taskStatus.getOpName());
//        int operator_threshold = (int) (cfg.getBufferSize() * scale_up_portion);
//        if (operator_threshold < taskStatus.getPendingTaskCnt()) { //if op who has the least pendingTaskLength still exceed threshold, need scale up
//            return false;
//        }
//        List<Tm.OperatorConfig.Builder> builders = upstream_operators.get(op_name); //get upstreamOperators
//        int reRouteCnt = ((builders.size() - 1) >> 1); //reRoute half upstream operators to new op
//        for (int i = 0; i < reRouteCnt; i++) {
//            String tmAddr = routeTable.get(builders.get(i).getName());
//            TMClient tmClient = new TMClient(tmAddr);
//            Tm.OperatorConfig.Builder prevConfig = builders.get(i);
//            for (int j = 0; j < prevConfig.getOutputMetadataCount(); i++) { //change downStream operators to new op
//                Tm.OutputMetadata outputMeta = prevConfig.getOutputMetadata(j);
//                if (!outputMeta.getName().equals(op_name)) continue;
//                Tm.OutputMetadata newMeta = Tm.OutputMetadata.newBuilder()
//                        .setName(taskStatus.getOpName())
//                        .setAddress(taskStatus.getTmAddr())
//                        .build();
//                prevConfig.setOutputMetadata(j, newMeta);
//                taskStatus.setUpstreamOperatorCnt(taskStatus.getUpstreamOperatorCnt() + 1);
//            }
//            upstream_operators.get(taskStatus.getOpName()).add(prevConfig);
//            upstream_operators.get(op_name).remove(i);
//            tmClient.reConfigOp(prevConfig.build()); //reConfig to new Op
//        }
        return true;
    }

    @Deprecated
    public void updatePQTaskStatus(PriorityBlockingQueue<OperatorTaskStatus> operatorTaskStatuses) {
//        PriorityBlockingQueue<OperatorTaskStatus> newOperatorTaskStatuses = new PriorityBlockingQueue<>();
//        while (!operatorTaskStatuses.isEmpty()) {
//            OperatorTaskStatus status = operatorTaskStatuses.poll();
//            TMClient tmClient = new TMClient(status.getTmAddr());
//            Pair<Integer, Integer> opQueueStatus = tmClient.getOpStatus(status.getOpName());
//            status.setPendingTaskCnt(opQueueStatus.getFirst());
//            newOperatorTaskStatuses.offer(status);
//        }
//        operatorTaskStatuses = newOperatorTaskStatuses;
//        return operatorTaskStatuses;
    }

    /**
     * get max of the min watermark from operator instances at the same type
     * @param op_instances
     * @return max of the min watermark
     */

    public long generateConsistentTimeStamp(List<OperatorTaskStatus> op_instances) {
        long low_watermark = Long.MIN_VALUE;
        int n = op_instances.size();
        for (int i = 0; i < n; i++) {
            String receiverOpName = op_instances.get(i).getOpName();
            TMClient tmClient = new TMClient(op_instances.get(i).getTmAddr());
            long watermark = tmClient.getOperatorLowWatermark(receiverOpName, "scaleUp");
            low_watermark = Math.max(low_watermark, watermark);
        }
        return (long) (low_watermark * (1 + cpcfg.consistent_control_grace_period));
    }

    public void sendExternalControlMessage(List<OperatorTaskStatus> op_instances) {
        int n = op_instances.size();
        // produce watermark
        long consistentTimeStamp = generateConsistentTimeStamp(op_instances);
        for (int i = 0; i < n; i++) {
            String receiverOpName = op_instances.get(i).getOpName();
            TMClient tmClient = new TMClient(op_instances.get(i).getTmAddr());
            tmClient.sendExternalControlMessage(receiverOpName, consistentTimeStamp, "scaleUp");

        }
    }

    public void sendReconfigControlMessage(ConcurrentHashMap<String, Tm.OperatorConfig.Builder> op_configs,
                                           List<OperatorLoadBalancer.OperatorTaskStatus> op_instances, TMClient tmClient){
        int n = op_instances.size();
        long consistentTimeStamp = generateConsistentTimeStamp(op_instances);

        Map<String, Tm.OperatorConfig> operatorConfigMap = new HashMap<>();
        for (Map.Entry<String, Tm.OperatorConfig.Builder> entry : op_configs.entrySet()) {
            String key = entry.getKey();
            Tm.OperatorConfig.Builder valueBuilder = entry.getValue();
            Tm.OperatorConfig value = valueBuilder.build();
            operatorConfigMap.put(key, value);
        }

        tmClient.sendReconfigControlMessage(operatorConfigMap ,consistentTimeStamp);
    }

    public Tm.OperatorConfig.Builder scaleUpOp(Tm.OperatorConfig.Builder cfg, TMClient tmClient) {
        String op_name = cfg.getName();
        String generalName = op_name.substring(0, op_name.length() - 2);
        Tm.OperatorConfig.Builder newCfg = Tm.OperatorConfig.newBuilder().setLogicalStage(cfg.getLogicalStage());
        List<OperatorTaskStatus> op_instances = operators_distribution.get(generalName);
        int instance_size = op_instances.size();
        sendExternalControlMessage(op_instances);

        newCfg.setName(generalName + "-" + instance_size).setBufferSize(cfg.getBufferSize()).setPartitionStrategy(cfg.getPartitionStrategy());
        newCfg.addAllOutputMetadata(cfg.getOutputMetadataList());
        upstream_operators.put(newCfg.getName(), upstream_operators.get(cfg.getName()));
        routeTable.put(newCfg.getName(), tmClient.getAddress());
        OperatorTaskStatus opTS = new OperatorTaskStatus(0, 0, newCfg.getName(), tmClient.getAddress());
        operators_distribution.get(generalName).add(opTS);
        BaseOperator op = hmBaseOperators.get(generalName);
        try {
            tmClient.addOperator(newCfg.build(), op);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        op_configs.put(newCfg.getName(), newCfg);

        // TODO: new way to sendReconfigControlMessage
        sendReconfigControlMessage(op_configs, op_instances, tmClient);

//        reRouteOperator(cfg.getName(), routeTable.get(cfg.getName()));
        List<Tm.OperatorConfig.Builder> builders = upstream_operators.get(op_name); //get upstreamOperators
        int reConfigCnt = builders.size(); //reRoute half upstream operators to new op
        for (int i = 0; i < reConfigCnt; i++) {
            String tmAddr = routeTable.get(builders.get(i).getName());
            TMClient client = new TMClient(tmAddr);
            Tm.OperatorConfig.Builder prevConfig = builders.get(i);
            Tm.OutputMetadata newMeta = Tm.OutputMetadata.newBuilder()
                    .setName(opTS.getOpName())
                    .setAddress(opTS.getTmAddr())
                    .build();
            prevConfig.addOutputMetadata(newMeta);
            upstream_operators.get(opTS.getOpName()).add(prevConfig);
            // FIXME: Reconfig no longer works this way
            throw new UnsupportedOperationException("Reconfig no longer works this way");
            //tmClient.reConfigOp(prevConfig.build()); //reConfig to new Op
        }
        return newCfg;
    }
}
