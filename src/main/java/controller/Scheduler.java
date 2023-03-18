package controller;

import kotlin.Pair;
import operators.BaseOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Tm;

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
        List<Pair<Tm.OperatorConfig, BaseOperator>> flatList = plan.getFlatList();
        while(i<flatList.size()){
            // read the clients on the fly since cluster membership may change
            String[] clients = (String[]) tmClients.keySet().toArray();

            // TODO: assign operator config's outputAddress and bufferSize
            
            for(int j=0; j<clients.length; j++){
                while(i<flatList.size()){
                    TMClient tmClient = tmClients.get(clients[j]);
                    BaseOperator op = plan.getOperators().get(i);
                    Tm.OperatorConfig cfg = plan.getStages().get(i).get(0);
                    try{
                        tmClient.addOperator(cfg, op);
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