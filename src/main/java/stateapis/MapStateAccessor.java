package stateapis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapStateAccessor<T> implements StateAccessor<T> {

    private final Map<String, BaseState> stateMap = new HashMap<>();
    private final Class<T> stateType;

    public MapStateAccessor(Class<T> stateType) {
        this.stateType = stateType;
    }

    @Override
    public BaseState getState(String key) throws InstantiationException, IllegalAccessException {
        BaseState state = stateMap.get(key);
        if (state == null) {
            state = new BaseState<>(stateType.newInstance());
            stateMap.put(key, state);
        }
        return state;
    }

    @Override
    public T value(String key) throws InstantiationException, IllegalAccessException {
        return StateAccessor.super.value(key);
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
    public List<String> getKeys() {
        return null;
    }

    @Override
    public List<T> getValues() {
        return StateAccessor.super.getValues();
    }


}
