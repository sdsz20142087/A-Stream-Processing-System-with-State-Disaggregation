package stateapis;

public class MapStateAccessor<T> implements State<T> {


    private final Class<T> stateType;
    private RemoteKVProvider remoteKVProvider = new RemoteKVProvider();

    public MapStateAccessor(Class<T> stateType) {
        this.stateType = stateType;
    }

    @Override
    public ValueState getState(String key) throws InstantiationException, IllegalAccessException {
        ValueState state = remoteKVProvider.get(key);
        if (state == null) {
            state = new ValueState<>(stateType.newInstance());
            remoteKVProvider.put(key, state);
        }
        return state;
    }

    @Override
    public void update(String key, T value) throws InstantiationException, IllegalAccessException {
        IStateAccessor.super.update(key, value);
    }

    @Override
    public void clear(String key) throws InstantiationException, IllegalAccessException {
        IStateAccessor.super.clear(key);
    }

    @Override
    public T value(String key) throws InstantiationException, IllegalAccessException {
        return (T) getState(key).value();
    }

}
