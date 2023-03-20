package controller;

import config.Config;
import config.TMConfig;
import exec.SerDe;
import operators.ISource;
import operators.SinkOperator;
import operators.SourceOperator;
import pb.Tm;
import utils.StringSerde;
import utils.WikiFileSource;

public class App {
    private static App instance;
    private final TMConfig tmcfg;
    private QueryPlan queryPlan;

    public static App getInstance() {
        if (instance == null)
            instance = new App();
        return instance;
    }

    private App(){
        this.queryPlan = new QueryPlan();
        this.tmcfg = Config.getInstance().taskManager;
        // TODO: build the query plan here
        ISource<String> src = new WikiFileSource("data.txt");
        SerDe<String> serde = new StringSerde();
        SourceOperator<String> source = new SourceOperator<>(src, serde);
        this.queryPlan.addStage(0, source, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);

        SinkOperator sink = new SinkOperator();
        this.queryPlan.addStage(1, sink, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }
}
