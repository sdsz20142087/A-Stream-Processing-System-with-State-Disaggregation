package controller;

import operators.BaseOperator;

public class Scheduler extends Thread{

    private QueryPlan plan;


    public Scheduler(QueryPlan plan){
        this.plan = plan;
        // TODO: analyze the existing planConfig and assign operators to TM
    }

    @Override
    public void run() {
        // TODO: deploy the query plan to TMs
        /*
        for (BaseOperator op : ops) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(op);
                byte[] bytes = baos.toByteArray();
                ByteString bs = ByteString.copyFrom(bytes);
                logger.info("got operator " + op.getName() + " bytes, size=" + bytes.length + "");

                Tm.AddOperatorRequest req = Tm.AddOperatorRequest
                        .newBuilder()
                        .setConfig(cfgSource)
                        .setObj(bs)
                        .build();
         */

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