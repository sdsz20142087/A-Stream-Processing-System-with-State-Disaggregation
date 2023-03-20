package controller;

import exec.SerDe;
import operators.ISource;
import operators.SinkOperator;
import operators.SourceOperator;
import pb.Tm;
import utils.StringSerde;
import utils.WikiFileSource;

public class App {
    private static App instance;
    private QueryPlan queryPlan;

    public static App getInstance() {
        if (instance == null)
            instance = new App();
        return instance;
    }

    private App(){
        this.queryPlan = new QueryPlan();
        // TODO: build the query plan here
        ISource<String> src = new WikiFileSource("data.txt");
        SerDe<String> serde = new StringSerde();
        SourceOperator<String> source = new SourceOperator<>(src, serde);
        this.queryPlan.addStage(source, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN);

        SinkOperator sink = new SinkOperator();
        this.queryPlan.addStage(sink, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN);
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }
}
