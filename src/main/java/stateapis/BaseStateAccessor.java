package stateapis;

/***
 * Represents a single piece of state that can be read, updated, or cleared.
 */

public abstract class BaseStateAccessor<T,T2> {
    protected String descriptorName;
    protected KVProvider kvProvider;
    protected IKeyGetter keyGetter;

    public BaseStateAccessor(String descriptorName, KVProvider kvProvider, IKeyGetter keyGetter) {
        this.descriptorName = descriptorName;
        this.kvProvider = kvProvider;
        this.keyGetter = keyGetter;
    }

    public abstract T value();

    public abstract void update(T2 value);

    public abstract void clear();
}
