package stateapis;


public class ValueStateAccessor<T> extends BaseStateAccessor<T> {

    public ValueStateAccessor(String descriptorName, KVProvider kvProvider, T defaultValue) {
        super(descriptorName, kvProvider);
    }

    @Override
    public T value() {
        T value = (T) kvProvider.get(descriptorName, 0);
        return value;
    }

    @Override
    public void update(T value) {
        kvProvider.put(descriptorName, value);
    }

    @Override
    public void clear() {
        kvProvider.delete(descriptorName);
    }

}
