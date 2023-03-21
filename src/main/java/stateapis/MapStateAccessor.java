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

