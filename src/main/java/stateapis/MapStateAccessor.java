package stateapis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapStateAccessor<T> implements StateAccessor<T> {


    private final Class<T> stateType;
    private RemoteKVProvider remoteKVProvider = new RemoteKVProvider();

    public MapStateAccessor(Class<T> stateType) {
        this.stateType = stateType;
    }

    @Override
    public BaseState getState(String key) throws InstantiationException, IllegalAccessException {
        BaseState state = remoteKVProvider.get(key);
        if (state == null) {
            state = new BaseState<>(stateType.newInstance());
            remoteKVProvider.put(key, state);
        }
        return state;
    }

    @Override
    public void update(String key, T value) throws InstantiationException, IllegalAccessException {
        StateAccessor.super.update(key, value);
    }

    @Override
    public void clear(String key) throws InstantiationException, IllegalAccessException {
        StateAccessor.super.clear(key);
    }

    @Override
    public T value(String key) throws InstantiationException, IllegalAccessException {
        return (T) getState(key).value();
    }

}
