package stateapis;

import java.util.List;
import java.util.Set;

public interface IDataflowMap<K, V> {
    V get(K key);
    void put(K key, V value);
    void remove(K key);
    void clear();
    List<K> keys();

    boolean isEmpty();

    boolean containsKey(K key);

    int size();
}
