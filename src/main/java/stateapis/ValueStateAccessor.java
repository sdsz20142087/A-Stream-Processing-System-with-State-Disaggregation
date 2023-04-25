package stateapis;


public class ValueStateAccessor<T> extends BaseStateAccessor<T, T> {

    private T defaultValue;

    public ValueStateAccessor(String descriptorName, KVProvider kvProvider, T defaultValue, IKeyGetter keyGetter) {
        super(descriptorName, kvProvider, keyGetter);
        this.defaultValue = defaultValue;
    }

    private String makeKey(){
        String key = keyGetter.getCurrentKey();
        String stateKey = descriptorName + key;
        return stateKey;
    }

    @Override
    public T value() {
        T value = (T) kvProvider.get(makeKey(), defaultValue);
        return value;
    }

    @Override
    public void update(T value) {
        kvProvider.put(makeKey(), value);
    }

    @Override
    public void clear() {
        kvProvider.delete(makeKey());
    }

}
