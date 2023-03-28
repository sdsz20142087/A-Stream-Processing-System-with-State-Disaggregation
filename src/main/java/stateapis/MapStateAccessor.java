package stateapis;

import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
public class MapStateAccessor<V> extends BaseStateAccessor<IDataflowMap<V>> {
    private MapProxy<V> mapProxy;

    public MapStateAccessor(java.lang.String descriptorName, KVProvider kvProvider) {
        super(descriptorName, kvProvider);
        mapProxy = new MapProxy<>(descriptorName, kvProvider);
    }

    @Override
    /*
    value() returns a proxy object, the object MUST NOT be null, the rest of the operations
    on the mapstate should be done on the proxy.
     */
    public IDataflowMap<V> value() {
        return this.mapProxy;
    }

    @Override
    /*
    update re-writes the entire new map, equivalent to PUT
     */
    public void update(IDataflowMap<V> value) {
        for(String key: value.keys()){
            this.mapProxy.put(key, value.get(key));
        }
    }

    /*
    clear() removes all the keys in the mapproxy
     */
    @Override
    public void clear() {
        this.mapProxy.clear();
    }
}


// mapproxy translates the map interface to the kvprovider interface

class MapProxy<V> implements IDataflowMap<V> {

    private final String keyBase;
    private final KVProvider kvProvider;
    private String sizeKey;
    private final Logger logger = LogManager.getLogger();
    private String makeKey(String key){
        return this.keyBase + ":" + key;
    }

    public MapProxy(String keyBase, KVProvider kvProvider) {
        this.keyBase = keyBase;
        this.kvProvider = kvProvider;
        this.sizeKey = keyBase + ".size";
        if (kvProvider.listKeys(keyBase).size() == 0) {
            kvProvider.put(sizeKey, 0);
        }
    }

    private void setSize(int size) {
        kvProvider.put(sizeKey, size);
    }
    
    @Override
    public V get(String key) {
        String k = makeKey(key);
        return (V) this.kvProvider.get(k, null);
    }

    @Override
    public void put(String key, V value) {
        String k = makeKey(key);
        this.kvProvider.put(k, value);
    }

    @Override
    public void remove(String key) {
        if (this.isEmpty()) {
            throw new RuntimeException("Cannot remove from an empty map");
        }
        String k = makeKey(key);
        this.kvProvider.delete(k);
    }

    @Override
    public void clear() {
        this.kvProvider.clear(this.keyBase);
    }

    @Override
    public List<String> keys() {
        return this.kvProvider.listKeys(this.keyBase);
    }

    @Override
    public boolean isEmpty() {
        return this.keys().size()==0;
    }

    @Override
    public boolean containsKey(String key) {
        String k = makeKey(key);
        return this.kvProvider.get(k,0)!=null;
    }

    @Override
    public int size() {
        return (int) kvProvider.get(sizeKey, 0);
    }
}

