package stateapis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.HashMap;

public class ListStateAccessor<T> extends BaseStateAccessor<IDataflowDeque<T>, Deque<T>> {

    private DequeProxy<T> dequeProxy;

    public ListStateAccessor(String descriptorName, KVProvider kvProvider, IKeyGetter keyGetter) {
        super(descriptorName, kvProvider, keyGetter);
        this.dequeProxy = new DequeProxy<>(descriptorName, kvProvider, keyGetter);
    }

    /*
    value() returns a proxy object, the object MUST NOT be null
     */
    @Override
    public IDataflowDeque<T> value() {
        return this.dequeProxy;
    }

    @Override
    /*
    update consumes the value, re-writes the entire new list, equivalent to PUT
     */
    public void update(Deque<T> value) {
        IDataflowDeque<T> targetProxy = this.value();
        targetProxy.clear();
        for (int i = 0; i < value.size(); i++) {
            targetProxy.addLast(value.removeFirst());
        }
    }

    @Override
    public void clear() {
        IDataflowDeque<T> targetProxy = this.value();
        targetProxy.clear();
    }

}

// dequeproxy translates the deque interface to the kvprovider interface
class DequeProxy<T> implements IDataflowDeque<T> {
    private String keyBase;
    private KVProvider kvProvider;

    private IKeyGetter keyGetter;
    private final Logger logger = LogManager.getLogger();

    private String makeFrontIndexKey() {
        String k = keyBase + keyGetter.getCurrentKey();
        createIfNull(k);
        String frontIndex = k +".front";
        return frontIndex;
    }

    private String makeSizeKey() {
        String k = keyBase + keyGetter.getCurrentKey();
        createIfNull(k);
        String sizeKey = k +".size";
        return sizeKey;
    }

    private void createIfNull(String prefix){
        if(kvProvider.listKeys(prefix).size()==0){
            kvProvider.put(prefix+".front", 0);
            kvProvider.put(prefix+".size", 0);
        }
    }

    public DequeProxy(String keyBase, KVProvider kvProvider, IKeyGetter keyGetter) {
        this.keyBase = keyBase;
        this.kvProvider = kvProvider;
        this.keyGetter = keyGetter;
    }

    private int getFrontIndex() {
        return (int) kvProvider.get(makeFrontIndexKey(), 0);
    }

    private void setFrontIndex(int index) {
        kvProvider.put(makeFrontIndexKey(), index);
    }

    private void setSize(int size) {
        kvProvider.put(makeSizeKey(), size);
    }

    private String makeIndex(int idx) {
        return keyBase + keyGetter.getCurrentKey() + "." + idx;
    }

    @Override
    public void addFirst(T value) {
        int newFrontIndex = getFrontIndex() - 1;
        kvProvider.put(makeIndex(newFrontIndex), value);
        setSize(size() + 1);
        setFrontIndex(newFrontIndex);
    }

    @Override
    public void addLast(T value) {
        int nextIndex = getFrontIndex() + size();
        kvProvider.put(makeIndex(nextIndex), value);
        setSize(size() + 1);
    }

    @Override
    public T removeFirst() {
        if (this.isEmpty()) {
            throw new RuntimeException("Cannot remove from an empty deque");
        }
        int frontIndex = getFrontIndex();
        logger.info("frontIndex: " + makeIndex(frontIndex));
        T value = (T) kvProvider.get(makeIndex(frontIndex), null);
        kvProvider.delete(makeIndex(frontIndex));
        setFrontIndex(frontIndex + 1);
        setSize(size() - 1);
        return value;
    }

    @Override
    public T removeLast() {
        if (this.isEmpty()) {
            throw new RuntimeException("Cannot remove from an empty deque");
        }
        int lastIndex = getFrontIndex() + size() - 1;
        T value = (T) kvProvider.get(makeIndex(lastIndex), null);
        kvProvider.delete(makeIndex(lastIndex));
        setSize(size() - 1);
        return value;
    }

    @Override
    public T peekFirst() {
        if (size() == 0) {
            return null;
        }
        int frontIndex = getFrontIndex();
        return (T) kvProvider.get(makeIndex(frontIndex), null);
    }

    @Override
    public T peekLast() {
        if (size() == 0) {
            return null;
        }
        int lastIndex = getFrontIndex() + size() - 1;
        return (T) kvProvider.get(makeIndex(lastIndex), null);
    }

    @Override
    public void clear() {
        int frontIndex = getFrontIndex();
        int size = size();
        for (int i = frontIndex; i < size; i++) {
            kvProvider.delete(makeIndex(i));
        }
        setFrontIndex(0);
        setSize(0);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        return (int) kvProvider.get(makeSizeKey(), 0);
    }
}
