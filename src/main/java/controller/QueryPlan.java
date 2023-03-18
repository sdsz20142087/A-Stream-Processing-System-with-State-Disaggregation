package controller;

import kotlin.Pair;
import operators.BaseOperator;
import pb.Tm;

import java.util.ArrayList;
import java.util.List;

public class QueryPlan {

    private List<List<Tm.OperatorConfig>> stages;
    private List<BaseOperator> operators;

    public QueryPlan() {
        this.stages = new ArrayList<>();
    }

    // We are assuming that the first item of the planConfig is source, and last is sink
    // The outputAddress and bufferSize are adjusted by the scheduler after the plan is submitted
    public QueryPlan addStage(BaseOperator op, int parallelism, Tm.PartitionStrategy partitionStrategy) {
        List<Tm.OperatorConfig> list = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            Tm.OperatorConfig cfg = Tm.OperatorConfig.newBuilder()
                    .setName(op.getName() + "-" + i)
                    .setPartitionStrategy(partitionStrategy)
                    .build();
            list.add(cfg);
        }
        operators.add(op);
        stages.add(list);
        return this;
    }

    public List<List<Tm.OperatorConfig>> getStages() {
        return stages;
    }

    public List<BaseOperator> getOperators() {
        return operators;
    }

    public List<Pair<Tm.OperatorConfig, BaseOperator>> getFlatList() {
        List<Pair<Tm.OperatorConfig, BaseOperator>> list = new ArrayList<>();
        for (int i = 0; i < stages.size(); i++) {
            for (int j = 0; j < stages.get(i).size(); j++) {
                list.add(new Pair<>(stages.get(i).get(j), operators.get(i)));
            }
        }
        return list;
    }
}
