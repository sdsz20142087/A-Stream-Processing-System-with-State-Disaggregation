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
        List<Triple<Integer, Tm.OperatorConfig, BaseOperator>> flatList = plan.getFlatList();
        int stageDepth = plan.getStages().size();
        HashMap<Integer, List<String>> stageToTm = new HashMap<>();
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
                    Triple<Integer, Tm.OperatorConfig, BaseOperator> item = flatList.get(flatList.size()-1-i);
                    if(item.getFirst() != stageDepth-1){
                        // assign the outputAddress and bufferSize
                        // FIXME: in cases like keyby, the stream is not fully connected between stages, extra impl is needed
                        Tm.OperatorConfig cfg = item.getSecond();
                        cfg.getOutputAddressList().addAll(stageToTm.get(item.getFirst()+1));
                    }
                    try{
                        tmClient.addOperator(item.getSecond(), item.getThird());
                        stageToTm.get(item.getFirst()).add(tmClient.getAddress());
                        i++;
                    } catch (Exception e){
                        logger.error("Failed to add operator to TM at " + tmClient.getHost()+tmClient.getPort(), e);
                        break;
                    }
                }
            }

            logger.warn("Did not sucessfully deploy all operators to TMs. Retrying in 2 seconds...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        // TODO: check the status and see if scaling is needed
        while(true){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}