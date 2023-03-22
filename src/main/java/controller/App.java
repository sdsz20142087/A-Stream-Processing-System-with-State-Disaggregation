package controller;

import config.Config;
import config.TMConfig;
import utils.SerDe;
import operators.ISource;
import operators.SinkOperator;
import operators.SourceOperator;
import pb.Tm;
import utils.StringSerde;
import utils.WikiFileSource;

import java.util.ArrayList;
import java.util.List;

public class App {
    private static App instance;
    private final TMConfig tmcfg;
    private QueryPlan queryPlan;

    public static App getInstance() {
        if (instance == null)
            instance = new App();
        return instance;
    }

    private App() {
        this.queryPlan = new QueryPlan();
        this.tmcfg = Config.getInstance().taskManager;
        // TODO: build the query plan here
        ISource<String> src = new WikiFileSource("data.txt");
        SerDe<String> serde = new StringSerde();
        SourceOperator<String> source = new SourceOperator<>(src, serde);

        // FIXME: the stages should actually be topologically sorted
        this.queryPlan.addStage(0, source, 3, 3, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);

        SinkOperator sink = new SinkOperator();
        this.queryPlan.addStage(1, sink, 3, 3, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);
        List<String> downStreamNames = new ArrayList<>();
        downStreamNames.add(sink.getOpName());
        this.queryPlan.addDownStreamOp(0, source, downStreamNames);
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }
}
