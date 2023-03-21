package stateapis;

public interface KVProvider {
    Object get(String stateKey);
    void put(String stateKey, Object rawObject);

    void delete(String stateKey);
}


