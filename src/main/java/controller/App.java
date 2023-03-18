package controller;

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
    }

    public QueryPlan getQueryPlan() {
        return queryPlan;
    }
}
