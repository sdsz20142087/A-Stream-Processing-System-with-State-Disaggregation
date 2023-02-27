package exec;

public class DataStream<T>{

    private ExecEnv<T> env;


    public DataStream(ExecEnv<T> env){
        this.env = env;
    }

    public DataStream<T> print(){
        // TODO: add print to the query plan
        return this;
    }

    public DataStream<T> filter(){
        return null;
    }

    public DataStream<T> flatMap(){
        return null;
    }

    public DataStream<T> keyBy(){
        return null;
    }
}
