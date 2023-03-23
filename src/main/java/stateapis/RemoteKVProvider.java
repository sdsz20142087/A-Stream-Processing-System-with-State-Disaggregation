package stateapis;

public class RemoteKVProvider implements KVProvider{

    @Override
    public String get(String stateKey, Object defaultValue) {
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
    public void clear(String prefix) {

    }
}
