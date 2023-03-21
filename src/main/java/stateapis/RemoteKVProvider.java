package stateapis;

import java.util.HashMap;
import java.util.Map;

public class RemoteKVProvider implements KVProvider{

    @Override
    public String get(String stateKey) {
        return null;
    }

    @Override
    public void put(String stateKey, String serializedState) {

    }

    @Override
    public void delete(String stateKey) {

    }
}
