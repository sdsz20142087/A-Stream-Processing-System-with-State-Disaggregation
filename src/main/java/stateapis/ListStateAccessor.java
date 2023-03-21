package stateapis;

public class ListStateAccessor<T> extends BaseStateAccessor<IDataflowDeque<T>> {

    public ListStateAccessor(String descriptorName, KVProvider kvProvider) {
        super(descriptorName, kvProvider);
    }

    /*
    value() returns a proxy object, the object MUST NOT be null
     */
    @Override
    public IDataflowDeque<T> value() {
        return new DequeProxy<>(descriptorName, this.kvProvider);
    }

    @Override
    /*
    update re-writes the entire new list, equivalent to PUT
     */
    public void update(IDataflowDeque<T> value) {
        // TODO: implement this
    }

    @Override
    public void clear() {
        // TODO: implement this
    }

}

// dequeproxy translates the deque interface to the kvprovider interface
class DequeProxy<T> implements IDataflowDeque<T> {
    private String keyBase;
    private KVProvider kvProvider;

    public DequeProxy(String keyBase, KVProvider kvProvider) {
        this.keyBase = keyBase;
        this.kvProvider = kvProvider;
    }

    @Override
    public void addFirst(T value) {

    }

    @Override
    public void addLast(T value) {

    }

    @Override
    public T removeFirst() {
        return null;
    }

    @Override
    public T removeLast() {
        return null;
    }

    @Override
    public T peekFirst() {
        return null;
    }

    @Override
    public T peekLast() {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }
}
