package stateapis;

/***
 * Represents a single piece of state that can be read, updated, or cleared.
 */
public interface State<T>{

    /**
     * Returns the current value of the state.
     */
    T value();

    /**
     * Updates the state with the given value.
     */
    void update(T value);

    /**
     * Clears the state, resetting it to its default value.
     */
    void clear();
}

