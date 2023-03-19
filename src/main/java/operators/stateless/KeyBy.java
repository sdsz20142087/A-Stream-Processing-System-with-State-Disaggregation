package operators.stateless;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Tm;

import java.io.Serializable;

public class KeyBy<T,K> extends BaseOperator implements Serializable {
    private SerDe<T> serde;
    private  K key; // position or name

    private int numOfPartitions;



    public KeyBy(int numOfPartitions, SerDe<T> serde) {
        this.serde = serde;
        this.numOfPartitions = numOfPartitions;
    }

    @Override
    protected void processElement(ByteString in) {
        T data = serde.deserialize(in);
        int partition = data.hashCode()%getNumOfPartitions();

    }

    public void setKey(K key){
        this.key = key;
    }
    private int getNumOfPartitions(){
        return this.numOfPartitions;
    }
}
