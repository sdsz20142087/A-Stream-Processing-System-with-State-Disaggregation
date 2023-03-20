package controller;

import kotlin.Triple;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Tm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class OperatorLoadBalancer{
    //Todo: corresponding to each operator, control multiple instance of this operator
    //Todo: maybe just need a LoadBalancer, control all operators, like <<op1.1, op1.2>, <op2.1, op2.2, op2.3>>, store config<TM addr, opName>
    //Todo: QueryPlan, init OperatorLoadBalancer, when use stage to add each operator output, call this function and choose an operator
    //Todo: change QueryPlan stage level, now each stage contains <op1.1, op1.2>, we need <OP1LB, OP2LB>, call LB get an operator and add.
    //Todo: scale up, OperatorLoadBalancer like a server, TM send request to LB, if pendingLength > threshold, reroute to another instance(same type)
    //Todo: scale up, if all instance > threshold, send to CP to scale up and reroute, need consistent hashing(or make it simple, bind to an instance)
    //Todo: then reroute, don't need to balance each operator workload.
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
    private ConcurrentHashMap<Pair<Integer, String>, List<Pair<String, String>>> operators_distribution = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, List<Tm.OutputMetadata>> stageToTm = new ConcurrentHashMap<>();
    private QueryPlan plan;
    private int stageDepth;
    private List<QueryPlan.OperatorInfo> flatList;
    private Logger logger = LogManager.getLogger();

    private OperatorLoadBalancer(QueryPlan plan) {
        this.plan = plan;
        this.flatList = plan.getFlatList();
        this.stageDepth = plan.getStages().size();
        for(int j=0; j<stageDepth; j++){
            stageToTm.put(j, new ArrayList<>());
        }
    }

    public void registerOperator(int stageIdx, int stageMaxDepth, Tm.OperatorConfig.Builder cfg, BaseOperator op, TMClient tmClient) {
        Tm.OperatorConfig.Builder cfgBuilder = cfg;
        if (stageIdx != stageDepth - 1) {
            cfgBuilder.addAllOutputMetadata(stageToTm.get(stageIdx + 1));
        }
        try {
            String op_name = cfgBuilder.getName();
            List<Pair<String, String>> op_instances = operators_distribution.get(new Pair<>(stageIdx, op_name));
            int instance_size = op_instances.size();
            cfgBuilder.setName(cfg.getName() + "-" + instance_size);
            Tm.OutputMetadata meta = Tm.OutputMetadata.
                    newBuilder().
                    setAddress(tmClient.getAddress()).
                    setName(cfg.getName()).
                    build();
            tmClient.addOperator(cfgBuilder.build(), op);
            operators_distribution.get(new Pair<>(stageIdx, op_name)).add(new Pair<>(cfg.getName(), tmClient.getAddress()));
            stageToTm.get(stageIdx).add(meta);
        } catch (Exception e) {
            logger.error("Failed to add operator to TM at " + tmClient.getHost() + tmClient.getPort(), e);
        }
    }

}
