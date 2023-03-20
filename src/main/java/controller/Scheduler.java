package controller;

import kotlin.Pair;
import kotlin.Triple;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Tm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class Scheduler extends Thread{

    private QueryPlan plan;
    private HashMap<String, TMClient> tmClients;
    // auto-increment
    private HashMap<Class<? extends BaseOperator>, Integer> operatorIdMap = new HashMap<>();
    private Logger logger = LogManager.getLogger();

    public Scheduler(QueryPlan plan, HashMap<String, TMClient> tmClients){
        this.plan = plan;
        this.tmClients = tmClients;
        // TODO: analyze the existing planConfig and assign operators to TM
    }

    @Override
    public void run() {
        // TODO: deploy the query plan to TMs
        // FIXME: naive implementation
        int i=0;
        List<Triple<Integer, Tm.OperatorConfig.Builder, BaseOperator>> flatList = plan.getFlatList();
        int stageDepth = plan.getStages().size();
        HashMap<Integer, List<Tm.OutputMetadata>> stageToTm = new HashMap<>();
        for(int j=0; j<stageDepth; j++){
            stageToTm.put(j, new ArrayList<>());
        }
        while(i<flatList.size()){
            // read the clients on the fly since cluster membership may change
            Collection<TMClient> clients = tmClients.values();

            // TODO: assign operator config's outputAddress and bufferSize
            for(TMClient tmClient: clients){
                while(i<flatList.size()){
                    // !! deploy the operators in reverse order so we know the outputAddress of the next stage
                    Triple<Integer, Tm.OperatorConfig.Builder, BaseOperator> item = flatList.get(flatList.size()-1-i);
                    if(item.getFirst() != stageDepth-1){
                        // assign the outputAddress and bufferSize
                        // FIXME: in cases like keyby, the stream is not fully connected between stages, extra impl is needed
                        Tm.OperatorConfig.Builder cfgBuilder = item.getSecond();
                        cfgBuilder.addAllOutputMetadata(stageToTm.get(item.getFirst()+1));
                    }
                    try{
                        // assign a real name first
                        BaseOperator op = item.getThird();
                        this.operatorIdMap.put(op.getClass(), this.operatorIdMap.getOrDefault(op.getClass(), 0)+1);
                        String realName = op.getName()+"_"+this.operatorIdMap.get(op.getClass());
                        op.setOpName(realName);
                        Tm.OperatorConfig.Builder cfgBuilder = item.getSecond();
                        cfgBuilder.setName(realName);
                        tmClient.addOperator(cfgBuilder.build(), op);
                        Tm.OutputMetadata meta = Tm.OutputMetadata.
                                newBuilder().
                                setAddress(tmClient.getAddress()).
                                setName(item.getSecond().getName()).
                                build();
                        stageToTm.get(item.getFirst()).add(meta);
                        i++;
                    } catch (Exception e){
                        logger.error("Failed to add operator to TM at " + tmClient.getHost()+tmClient.getPort(), e);
                        break;
                    }
                }
            }
            if(i<flatList.size()){
                logger.warn("Did not sucessfully deploy all operators to TMs. Retrying in 2 seconds...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }
        }


        // TODO: check the status and see if scaling is needed
        while(true){
            try {
                logger.info("CP: check status");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}