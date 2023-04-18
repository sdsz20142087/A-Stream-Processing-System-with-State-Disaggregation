package stateapis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ListStateAccessor<T> extends BaseStateAccessor<IDataflowDeque<T>> {

    private DequeProxy<T> dequeProxy;

    public ListStateAccessor(String descriptorName, KVProvider kvProvider) {
        super(descriptorName, kvProvider);
        this.dequeProxy = new DequeProxy<>(descriptorName, kvProvider);
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
    public void update(IDataflowDeque<T> value) {
        this.dequeProxy.clear();
        for (int i = 0; i < value.size(); i++) {
            this.dequeProxy.addLast(value.removeFirst());
        }
    }

    @Override
    public void clear() {
        this.dequeProxy.clear();
    }

}

// dequeproxy translates the deque interface to the kvprovider interface
class DequeProxy<T> implements IDataflowDeque<T> {
    private String keyBase;
    private KVProvider kvProvider;

    private String frontIndexKey;
    private String sizeKey;
    private T sum;
    private final Logger logger = LogManager.getLogger();

    public DequeProxy(String keyBase, KVProvider kvProvider) {
        this.keyBase = keyBase;
        this.kvProvider = kvProvider;
        this.frontIndexKey = keyBase + ".front";
        this.sizeKey = keyBase + ".size";
        if (kvProvider.listKeys(keyBase).size() == 0) {
            kvProvider.put(frontIndexKey, 0);
            kvProvider.put(sizeKey, 0);
        }
    }

    private int getFrontIndex() {
        return (int) kvProvider.get(frontIndexKey, 0);
    }

    private void setFrontIndex(int index) {
        kvProvider.put(frontIndexKey, index);
    }

    private void setSize(int size) {
        kvProvider.put(sizeKey, size);
    }

    private String makeIndex(int idx) {
        return keyBase + "." + idx;
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
        if(size()==0){
            return null;
        }
        int frontIndex = getFrontIndex();
        return (T) kvProvider.get(makeIndex(frontIndex), null);
    }

    @Override
    public T peekLast() {
        if(size()==0){
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
    public T getSum() {
        return sum;
    }


    @Override
    public int size() {
        return (int) kvProvider.get(sizeKey, 0);
    }
}
