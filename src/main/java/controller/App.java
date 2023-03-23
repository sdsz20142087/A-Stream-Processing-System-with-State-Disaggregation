package controller;

import config.Config;
import config.TMConfig;
import operators.stateful.CountOperator;
import operators.stateless.Filter;
import utils.*;
import operators.SinkOperator;
import operators.SourceOperator;
import pb.Tm;

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
        // FIXME: the stages should actually be topologically sorted
        // 1: source
        SourceOperator<String> source = new SourceOperator<>(new WikiFileSource("data.txt"), new StringSerde());
        this.queryPlan.addStage(0, source, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);

        // 2: filter: wikiInfo.id % 4 != 0;
        Filter<WikiInfo> filter = new Filter<>(new WikiInfoSerde(), new WIKIPredicate());
        this.queryPlan.addStage(1, filter, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);

        // 3: count
        CountOperator count = new CountOperator(new StringSerde());
        this.queryPlan.addStage(2, count, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);

        // 4: sink
        SinkOperator sink = new SinkOperator();
        this.queryPlan.addStage(3, sink, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);



//        SinkOperator sink = new SinkOperator();
//        this.queryPlan.addStage(1, sink, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);
//        List<String> downStreamNames = new ArrayList<>();
//        downStreamNames.add(sink.getOpName());
//        this.queryPlan.addDownStreamOp(0, source, downStreamNames);
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }
}
