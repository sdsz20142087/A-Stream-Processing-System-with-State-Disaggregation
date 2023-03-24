package stateapis;

import java.util.List;
import java.util.Set;

public interface IDataflowMap<V> {
    // The type of keys must be String
    V get(String key);
    void put(String key, V value);
    void remove(String key);
    void clear();
    List<String> keys();

    boolean isEmpty();

    boolean containsKey(String key);

    int size();
}
