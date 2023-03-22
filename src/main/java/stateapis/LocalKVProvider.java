package stateapis;

public class LocalKVProvider implements KVProvider{
    @Override
    public Object get(String stateKey) {
        return null;
    }

    @Override
    public void put(String stateKey, Object rawObject) {

    }

    @Override
    public void put(String stateKey, byte[] value) {

    }

    @Override
    public void delete(String stateKey) {

    }

    @Override
    public void clear() {

    }
}
