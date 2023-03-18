package stateapis;

import java.util.List;

/***
 * Provides methods for accessing and manipulating multiple pieces of state associated with different keys.
 * @param <T>
 */
public interface StateAccessor<T> {

    /**
     * Returns the state associated with the given key.
     */
    BaseState getState(String key) throws InstantiationException, IllegalAccessException;

    /**
     * Returns the current value of the state associated with the given key.
     */
    default T value(String key) throws InstantiationException, IllegalAccessException {
        return (T) getState(key).value();
    }

    /**
     * Updates the state associated with the given key with the given value.
     */
    default void update(String key, T value) throws InstantiationException, IllegalAccessException {
        getState(key).update(value);
    }

    /**
     * Clears the state associated with the given key, resetting it to its default value.
     */
    default void clear(String key) throws InstantiationException, IllegalAccessException {
        getState(key).clear();
    }

    List<String> getKeys();

    default List<T> getValues() {
        return null;
    }

}
