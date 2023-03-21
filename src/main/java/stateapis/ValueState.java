package stateapis;


public class ValueState<T> implements State<T> {

    private String descriptorName;

    private KVProvider kvProvider;

    public ValueState(String descriptorName, KVProvider kvProvider) {
        this.descriptorName = descriptorName;
        this.kvProvider = kvProvider;
    }
    @Override
    public T value() {
        T value = (T) kvProvider.get(descriptorName);
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
