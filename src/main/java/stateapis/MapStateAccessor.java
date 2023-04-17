package stateapis;

import java.util.HashMap;
import java.util.List;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MapStateAccessor<V> extends BaseStateAccessor<IDataflowMap<V>> {
    private MapProxy<V> mapProxy;

    public MapStateAccessor(java.lang.String descriptorName, KVProvider kvProvider, IKeyGetter keyGetter) {
        super(descriptorName, kvProvider, keyGetter);
        mapProxy = new MapProxy<>(descriptorName, kvProvider, keyGetter);
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
        MapProxy<V> targetProxy = (MapProxy<V>) this.value();
        for (String key : value.keys()) {
            targetProxy.put(key, value.get(key));
        }
    }

    /*
    clear() removes all the keys in the mapproxy
     */
    @Override
    public void clear() {
        MapProxy<V> targetProxy = (MapProxy<V>) this.value();
        targetProxy.clear();
    }
}


// mapproxy translates the map interface to the kvprovider interface

class MapProxy<V> implements IDataflowMap<V> {

    private final String keyBase;
    private final KVProvider kvProvider;
    private final Logger logger = LogManager.getLogger();

    private IKeyGetter keyGetter;

    private String makeKey(String key) {
        String currentKey = keyGetter.getCurrentKey();
        return this.keyBase + ":" + key + (currentKey==null?"":":"+currentKey);
    }

    public MapProxy(String keyBase, KVProvider kvProvider, IKeyGetter keyGetter) {
        this.keyBase = keyBase;
        this.kvProvider = kvProvider;
        this.keyGetter = keyGetter;
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

        this.kvProvider.delete(makeKey(key));
    }

    @Override
    public void clear() {
        this.kvProvider.clear(makeKey(""));
    }

    @Override
    public List<String> keys() {
        return this.kvProvider.listKeys(makeKey(""));
    }

    @Override
    public boolean isEmpty() {
        return this.keys().size() == 0;
    }

    @Override
    public boolean containsKey(String key) {
        String k = makeKey(key);
        return this.kvProvider.get(k, null) != null;
    }

    @Override
    public int size() {
        return kvProvider.listKeys(makeKey("")).size();
    }
}

