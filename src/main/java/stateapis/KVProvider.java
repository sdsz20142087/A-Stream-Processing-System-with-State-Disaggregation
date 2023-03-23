package stateapis;

public interface KVProvider {
    Object get(String stateKey, Object defaultValue);
    void put(String stateKey, Object rawObject);

    void put(String stateKey, byte[] value);

    void delete(String stateKey);

    void clear(String prefix);
}


