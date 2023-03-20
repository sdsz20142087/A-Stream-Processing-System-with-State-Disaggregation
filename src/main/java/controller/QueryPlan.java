package controller;

import kotlin.Pair;
import kotlin.Triple;
import operators.BaseOperator;
import pb.Tm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class QueryPlan {

//    private List<List<Tm.OperatorConfig.Builder>> stages;
    static class OperatorInfo{
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
    private ConcurrentHashMap<Integer, List<OperatorInfo>> stages;
//    private List<Pair<Integer, BaseOperator>> operators;

    public QueryPlan() {
//        this.stages = new ArrayList<>();
        this.stages = new ConcurrentHashMap<>();
//        this.operators = new ArrayList<>();
    }

    // We are assuming that the first item of the planConfig is source, and last is sink
    // The outputAddress and bufferSize are adjusted by the scheduler after the plan is submitted
    public QueryPlan addStage(int stageIdx, BaseOperator op, int parallelism, int parallelMax, Tm.PartitionStrategy partitionStrategy, int bufferSize) {
//        List<Tm.OperatorConfig.Builder> list = new ArrayList<>();
//        for (int i = 0; i < parallelism; i++) {
//            Tm.OperatorConfig.Builder cfg = Tm.OperatorConfig.newBuilder()
//                    .setName(op.getName() + "-" + i)
//                    .setPartitionStrategy(partitionStrategy)
//                    .setBufferSize(bufferSize)
//                    .addAllOutputMetadata(new ArrayList<>());
//            list.add(cfg);
//        }
//        operators.add(op);
//        stages.add(stageIdx, list);
        Tm.OperatorConfig.Builder cfg = Tm.OperatorConfig.newBuilder()
                .setName(op.getName())
                .setPartitionStrategy(partitionStrategy)
                .setBufferSize(bufferSize)
                .addAllOutputMetadata(new ArrayList<>());
//        operators.add(new Pair<>(stageIdx, op));
        stages.get(stageIdx).add(new OperatorInfo(stageIdx, parallelism, cfg, op));

        return this;
    }

//    public List<List<Tm.OperatorConfig.Builder>> getStages() {
//        return stages;
//    }

    public ConcurrentHashMap<Integer, List<OperatorInfo>> getStages() {
        return stages;
    }


    public List<OperatorInfo> getFlatList() {
        List<OperatorInfo> list = new ArrayList<>();
        for (Integer key : stages.keySet()) {
            List<OperatorInfo> stageOperators = stages.get(key);
            list.addAll(stageOperators);
        }

//        for (int i = 0; i < stages.size(); i++) {
//            for (int j = 0; j < stages.get(i).size(); j++) {
//                list.add(new Triple<>(i, stages.get(i).get(j), operators.get(i)));
//            }
//        }
        return list;
    }
}
