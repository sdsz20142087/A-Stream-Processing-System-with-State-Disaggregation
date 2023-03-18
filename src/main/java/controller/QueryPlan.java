package controller;

import operators.BaseOperator;
import pb.Tm;

import java.util.ArrayList;
import java.util.List;

public class QueryPlan {

    private List<List<Tm.OperatorConfig>> planConfig;

    public QueryPlan() {
        this.planConfig = new ArrayList<>();
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
        planConfig.add(list);
        return this;
    }
}
