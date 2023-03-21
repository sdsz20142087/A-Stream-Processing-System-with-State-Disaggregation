package stateapis;

import java.util.List;

public class MapStateAccessor<K, V> extends BaseStateAccessor<IDataflowMap<K, V>> {

    public MapStateAccessor(String descriptorName, KVProvider kvProvider) {
        super(descriptorName, kvProvider);
    }

    /*
    value() returns a proxy object, the object MUST NOT be null
     */
    @Override
    public IDataflowMap<K, V> value() {
        return new MapProxy<>(descriptorName, this.kvProvider);
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
    /*
    update re-writes the entire new map, equivalent to PUT
     */
    public void update(IDataflowMap<K, V> value) {
        // FIXME: this should be atomic
        for (K key : value.keys()) {
            this.kvProvider.put(descriptorName + "." + key, value.get(key));
        }
    }

    @Override
    public void clear() {
        // TODO: implement this
    }
}


// mapproxy translates the map interface to the kvprovider interface

class MapProxy<K, V> implements IDataflowMap<K, V> {

    private String keyBase;
    private KVProvider kvProvider;

    public MapProxy(String keyBase, KVProvider kvProvider) {
        this.keyBase = keyBase;
        this.kvProvider = kvProvider;
    }


    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public void put(K key, V value) {

    }

    @Override
    public void remove(K key) {

    }

    @Override
    public void clear() {

    }

    @Override
    public List<K> keys() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }
}

