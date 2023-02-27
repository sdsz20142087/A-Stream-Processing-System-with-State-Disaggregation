package exec;

import operators.ISource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExecEnv<T> {

    private ISource<T> source;
    private final Logger logger = LogManager.getLogger();

    public ExecEnv(){
    }

    public DataStream<T> WithDataSource(ISource<T> source) {
        this.source = source;
        return new DataStream<T>(this);
    }

    public void exec(){
        // TODO: finalize the query plan, deploy to the cluster
    }
}
