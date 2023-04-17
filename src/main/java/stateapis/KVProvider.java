package stateapis;

import pb.Tm;

import java.util.List;

public interface KVProvider {
    Object get(String stateKey, Object defaultValue);
    void put(String stateKey, Object rawObject);

    void put(String stateKey, byte[] value);

    List<String> listKeys(String prefix);

    void delete(String stateKey);

    void clear(String prefix);

    void close();

    void handleReconfig(Tm.ReconfigMsg msg);

    void addInvolvedOp(String opId, boolean hasKey);
    void removeInvolvedOp(String opId);

    void setLocalAddr(String addr);
}


