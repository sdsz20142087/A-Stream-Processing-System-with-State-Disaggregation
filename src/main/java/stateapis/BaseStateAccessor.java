package stateapis;

/***
 * Represents a single piece of state that can be read, updated, or cleared.
 */

public abstract class BaseStateAccessor<T>{
    protected String descriptorName;
    protected KVProvider kvProvider;

    public BaseStateAccessor(String descriptorName, KVProvider kvProvider) {
        this.descriptorName = descriptorName;
        this.kvProvider = kvProvider;
    }

    public abstract T value();

    public abstract void update(T value);
    public abstract void clear();
}
