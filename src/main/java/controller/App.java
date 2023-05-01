package controller;

import config.Config;
import config.TMConfig;
import operators.IKeySelector;
import operators.stateful.SingleCountOperator;
import operators.stateful.StatefulCPUHeavyOperator;
import operators.stateful.StrLenOperator;
import operators.stateless.Filter;
import utils.*;
import operators.SinkOperator;
import operators.SourceOperator;
import pb.Tm;

import java.io.Serializable;
import java.util.function.Predicate;

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
        SourceOperator<String> source = new SourceOperator<>(new WikiFileSource("data2.txt",7), new StringSerde());
        this.queryPlan.addStage(0, source, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);

        // 2: filter: wikiInfo.id % 4 != 0;
        Filter<WikiInfo> filter = new Filter<>(new WikiInfoSerde(), (Predicate<WikiInfo>& Serializable) wikiInfo -> wikiInfo.id % 4 != 0 );
        filter.setKeySelector(new WikiKeySelector());
        this.queryPlan.addStage(1, filter, 1, 1, Tm.PartitionStrategy.HASH, tmcfg.operator_bufferSize);

        // 3: stateful cpu heavy operator
        StatefulCPUHeavyOperator<WikiInfo> statefulCPUHeavyOperator = new StatefulCPUHeavyOperator<>(new WikiInfoSerde(), 5);
        this.queryPlan.addStage(2, statefulCPUHeavyOperator, 2, 2, Tm.PartitionStrategy.HASH, tmcfg.operator_bufferSize);

        // 4: count
        StrLenOperator strLenOperator = new StrLenOperator(new WikiInfoSerde(), new StringSerde(), 1);
        this.queryPlan.addStage(3, strLenOperator, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);

        // 5: sink
        SinkOperator sink = new SinkOperator(new StringSerde());
        this.queryPlan.addStage(4, sink, 1, 1, Tm.PartitionStrategy.ROUND_ROBIN, tmcfg.operator_bufferSize);
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }
}
