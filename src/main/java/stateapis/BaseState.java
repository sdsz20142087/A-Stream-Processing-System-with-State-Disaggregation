package stateapis;


public class BaseState<T> implements State<T> {

    private T value;

    public BaseState(T defaultValue) {
        this.value = defaultValue;
    }


    @Override
    public T value() {
        return this.value;
    }

    @Override
    public void update(T value) {
        this.value = value;
    }

    @Override
    public void clear() {
        this.value = null;
    }

}
