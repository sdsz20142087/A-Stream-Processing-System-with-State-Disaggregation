package stateapis;

/***
 * provides a way to obtain a StateAccessor for a specific namespace and state type.
 */
public interface StateBackend {

    /**
     * Returns a state accessor for the given namespace and state type.
     */
    <T> StateAccessor<T> getStateAccessor(String namespace, Class<T> stateType);
}
