package stateapis;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MapStateAccessor<K, V> extends BaseStateAccessor<Map> {
    private KVProvider kvProvider = new KVProvider();


    public MapStateAccessor(String descriptorName, KVProvider kvProvider) {
        super(descriptorName, kvProvider);
    }

    @Override
    public Map<K, V> value() {
        return new MapProxy<>(descriptorName);
    }

    @Override
    public Map<K, V> value() {
        Map<K, V> stateMap = new HashMap<>();

        // Retrieve the serialized state bytes from the key-value store
        byte[] stateBytes = kvProvider.get(descriptorName);

        if (stateBytes != null) {
            try {
                // Deserialize the state bytes into a Map object
                ByteArrayInputStream bis = new ByteArrayInputStream(stateBytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                stateMap = (Map<K, V>) ois.readObject();
                ois.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return stateMap;
    }

//    @Override
//    public void update(Map value) {
//        // FIXME: this should be atomic
//        for(Map.Entry<K, V> entry : (Iterable<Map.Entry<K, V>>) value.entrySet()){
//            remoteKVProvider.put(entry.getKey().toString(), entry.getValue().toString());
//        }
//    }

    @Override
    public void update(Map value) {
        try {
            // Serialize the Map object into bytes
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            byte[] stateBytes = bos.toByteArray();
            oos.close();
            kvProvider.put(descriptorName, stateBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clear() {
        kvProvider.clear();
    }
}
// TODO: implement this

class MapProxy<K,V> implements Map<K,V>{

    private String keyBase;

    public MapProxy(String keyBase){
        this.keyBase = keyBase;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @NotNull
    @Override
    public Set<K> keySet() {
        return null;
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return null;
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }
}

