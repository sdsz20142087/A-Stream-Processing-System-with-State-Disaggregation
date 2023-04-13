package operators.stateless;

import com.google.protobuf.ByteString;
import kotlin.NotImplementedError;
import operators.OutputSender;
import utils.SerDe;
import operators.BaseOperator;

import java.io.Serializable;

public class KeyBy<T,K> extends BaseOperator implements Serializable {
    private SerDe<T> serde;
    private  K key; // position or name

    private int numOfPartitions;



    public KeyBy(int numOfPartitions, SerDe<T> serde) {
        this.setName("KeyBy-");
        this.serde = serde;
        this.numOfPartitions = numOfPartitions;
    }

    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        T data = serde.deserialize(in);
        int partition = data.hashCode()%getNumOfPartitions();
        throw new NotImplementedError("Not implemented yet");
    }

    public void setKey(K key){
        this.key = key;
    }
    private int getNumOfPartitions(){
        return this.numOfPartitions;
    }
}
