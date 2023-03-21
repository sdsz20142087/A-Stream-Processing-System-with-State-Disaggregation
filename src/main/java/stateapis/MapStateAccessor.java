package stateapis;

import java.util.List;

public class MapStateAccessor<K, V> extends BaseStateAccessor<IDataflowMap<K, V>> {
    private RemoteKVProvider remoteKVProvider = new RemoteKVProvider();


    public MapStateAccessor(String descriptorName, KVProvider kvProvider) {
        super(descriptorName, kvProvider);
    }

    @Override
    public IDataflowMap<K, V> value() {
        return new MapProxy<>(descriptorName);
    }


    @Override
    public void update(IDataflowMap value) {
        // FIXME: this should be atomic
        for(Object key : value.keys()){
            remoteKVProvider.put(descriptorName + "." + key, value.get(key));
        }
    }

    @Override
    public void clear() {
        remoteKVProvider.clear();
    }
}
// TODO: implement this

class MapProxy<K,V> implements IDataflowMap<K,V>{

    private String keyBase;

    public MapProxy(String keyBase){
        this.keyBase = keyBase;
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

