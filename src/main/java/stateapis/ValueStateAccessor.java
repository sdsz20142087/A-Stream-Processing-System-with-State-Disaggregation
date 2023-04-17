package stateapis;


public class ValueStateAccessor<T> extends BaseStateAccessor<T> {

    private T defaultValue;

    public ValueStateAccessor(String descriptorName, KVProvider kvProvider, T defaultValue, IKeyGetter keyGetter) {
        super(descriptorName, kvProvider, keyGetter);
        this.defaultValue = defaultValue;
    }

    @Override
    public T value() {
        String key = keyGetter.getCurrentKey();
        String stateKey = descriptorName + ":" + (key == null ? "" : key);
        T value = (T) kvProvider.get(stateKey, defaultValue);
        return value;
    }

    @Override
    public void update(T value) {
        String key = keyGetter.getCurrentKey();
        String stateKey = descriptorName + ":" + (key == null ? "" : key);
        kvProvider.put(stateKey, value);
    }

    @Override
    public void clear() {
        String key = keyGetter.getCurrentKey();
        String stateKey = descriptorName + ":" + (key == null ? "" : key);
        kvProvider.delete(stateKey);
    }

}
