package stateapis;

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import pb.TMServiceGrpc;
import pb.Tm;
import utils.BytesUtil;

import java.util.List;

public class RemoteStateClient {


    private final String target;
    private final TMServiceGrpc.TMServiceBlockingStub blockingStub;
    public RemoteStateClient(String addr) {
        this.target = addr;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        blockingStub = TMServiceGrpc.newBlockingStub(channel);
    }

    public Object get(String key){
        Tm.GetStateResponse res = blockingStub.
                getState(Tm.GetStateRequest.newBuilder().setStateKey(key).build());
        return BytesUtil.ObjectFromBytes(res.getObj().toByteArray());
    }

    public void put(String key, byte[] value){
        blockingStub.updateState(Tm.UpdateStateRequest.
                newBuilder().setStateKey(key).setObj(ByteString.copyFrom(value)).build());
    }

    public List<String> listKeys(String prefix){
        Tm.ListKeysResponse res = blockingStub.listStateKeys(
                Tm.ListKeysRequest.newBuilder().setPrefix(prefix).build());
        return res.getKeysList();
    }

    public List<Tm.StateKV> pullStates(int stage, Tm.PartitionPlan plan){
        try{
            Thread.sleep(500);
        }catch (Exception e) {
            e.printStackTrace();
        }
        Tm.PullStatesResponse res = blockingStub.pullStates(
                Tm.PullStatesRequest.newBuilder().setLogicalStage(stage).setPartitionPlan(plan).build());
        return res.getStateKVsList();
    }

    public void delete(String key){
        blockingStub.removeState(Tm.RemoveStateRequest.newBuilder().setStateKey(key).build());
    }

    public void clear(String prefix){
        blockingStub.clearState(Tm.ClearStateRequest.newBuilder().setStateKey(prefix).build());
    }
}
