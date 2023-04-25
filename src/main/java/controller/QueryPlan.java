package controller;

import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Tm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class QueryPlan {
    static class OperatorInfo {
        public OperatorInfo(int stageIdx, int parallelism, Tm.OperatorConfig.Builder cfg, BaseOperator op) {
            this.stageIdx = stageIdx;
            this.parallelism = parallelism;
            this.cfg = cfg;
            this.op = op;
        }

        public int getStageIdx() {
            return stageIdx;
        }

        public void setStageIdx(int stageIdx) {
            this.stageIdx = stageIdx;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        public Tm.OperatorConfig.Builder getCfg() {
            return cfg;
        }

        public void setCfg(Tm.OperatorConfig.Builder cfg) {
            this.cfg = cfg;
        }

        public BaseOperator getOp() {
            return op;
        }

        public void setOp(BaseOperator op) {
            this.op = op;
        }

        private int stageIdx;
        private int parallelism;
        private Tm.OperatorConfig.Builder cfg;
        private BaseOperator op;
    }

    public HashMap<Class<? extends BaseOperator>, Integer> getOperatorIdMap() {
        return operatorIdMap;
    }

    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, OperatorInfo>> stages;
    private HashMap<Class<? extends BaseOperator>, Integer> operatorIdMap = new HashMap<>();
    private final Logger logger = LogManager.getLogger();

    public QueryPlan() {
        this.stages = new ConcurrentHashMap<>();
    }

    // We are assuming that the first item of the planConfig is source, and last is sink
    // The outputAddress and bufferSize are adjusted by the scheduler after the plan is submitted
    public QueryPlan addStage(int stageIdx, BaseOperator op, int parallelism, int parallelMax, Tm.PartitionStrategy partitionStrategy, int bufferSize) {
        logger.info("add " + op.getOpName() + "to query plan");
        this.operatorIdMap.put(op.getClass(), this.operatorIdMap.getOrDefault(op.getClass(), 0) + 1);
        String realName = op.getOpName() + "_" + this.operatorIdMap.get(op.getClass());
        op.setOpName(realName);
        Tm.OperatorConfig.Builder cfg = Tm.OperatorConfig.newBuilder()
                .setLogicalStage(stageIdx)
                .setName(op.getOpName())
                .setPartitionStrategy(partitionStrategy)
                .setBufferSize(bufferSize)
                .addAllOutputMetadata(new ArrayList<>());
        if (!stages.containsKey(stageIdx)) {
            stages.put(stageIdx, new ConcurrentHashMap<>());
        }
        stages.get(stageIdx).put(op.getOpName(), new OperatorInfo(stageIdx, parallelism, cfg, op));
        return this;
    }

    public ConcurrentHashMap<Integer, ConcurrentHashMap<String, OperatorInfo>> getStages() {
        return stages;
    }

    public void addDownStreamOp(int stageIdx, BaseOperator op, List<String> downStreamNames) {
        logger.info("load " + op.getOpName() + " downStream relations");
        List<Tm.OutputMetadata> downStreamMetaData = new ArrayList<>();
        for (int i = 0; i < downStreamNames.size(); i++) {
            Tm.OutputMetadata meta = Tm.OutputMetadata.
                    newBuilder().
                    setName(downStreamNames.get(i)).
                    build();
            downStreamMetaData.add(meta);
        }
        stages.get(stageIdx).get(op.getOpName()).getCfg().addAllOutputMetadata(downStreamMetaData);
    }


    public List<OperatorInfo> getFlatList() {
        List<OperatorInfo> list = new ArrayList<>();
        for (Integer key : stages.keySet()) {
            ConcurrentHashMap<String, OperatorInfo> stageOperators = stages.get(key);
            for (String name : stageOperators.keySet()) {
                list.add(stageOperators.get(name));
            }
        }
        return list;
    }
}
