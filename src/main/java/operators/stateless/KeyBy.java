package operators.stateless;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Op;

import java.io.Serializable;

public class KeyBy<T,K> extends BaseOperator implements Serializable {
    private SerDe<T> serde;
    private  K key; // position or name



    public KeyBy(Op.OperatorConfig config) {
        super(config);
        this.serde=serde;
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
        return super.getConfig().getNextOperatorAddressList().size();
    }
}
