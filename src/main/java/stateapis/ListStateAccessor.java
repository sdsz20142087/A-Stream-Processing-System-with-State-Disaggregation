package stateapis;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;

public class ListStateAccessor<T> extends BaseStateAccessor<IDataflowDeque<T>> {

    public ListStateAccessor(String descriptorName, KVProvider kvProvider){
        super(descriptorName,kvProvider);
    }

    @Override
    public IDataflowDeque<T> value() {
        return new DequeProxy<>(descriptorName);
    }

    @Override
    public void update(IDataflowDeque<T> value) {

    }

    @Override
    public void clear() {

    }

}

class DequeProxy<T> implements IDataflowDeque<T>{
    private String keyBase;
    public DequeProxy(String keyBase){
        this.keyBase = keyBase;
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


}
