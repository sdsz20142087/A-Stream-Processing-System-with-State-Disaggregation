package operators.stateless;

import com.google.protobuf.ByteString;
import kotlin.NotImplementedError;
import operators.OutputSender;
import utils.SerDe;
import operators.BaseOperator;

import java.io.Serializable;

public class KeyBy<T,K> extends BaseOperator implements Serializable {
    private  K key; // position or name

    private int numOfPartitions;



    public KeyBy(int numOfPartitions, SerDe<T> serde) {
        super(serde, serde);
        this.setName("KeyBy-");
        this.numOfPartitions = numOfPartitions;
    }

    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        T data = (T) serdeIn.deserializeIn(in);
        throw new NotImplementedError("Not implemented yet");
    }

    public void setKey(K key){
        this.key = key;
    }
    private int getNumOfPartitions(){
        return this.numOfPartitions;
    }
}
