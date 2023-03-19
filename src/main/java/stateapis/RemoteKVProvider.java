package stateapis;

import java.util.HashMap;
import java.util.Map;

public class RemoteKVProvider implements KVProvider{
    private final Map<String, BaseState> stateMap = new HashMap<>();

    @Override
    public BaseState get(String state) {
        return stateMap.get(state);
    }

    @Override
    public void put(String state, BaseState baseState) {
        stateMap.put(state, baseState);
    }

}
