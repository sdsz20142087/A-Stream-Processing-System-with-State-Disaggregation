package stateapis;

import java.util.Deque;

public class ListStateAccessor<T> extends BaseStateAccessor<Deque<T>> {
    private KVProvider kvProvider;

    public ListStateAccessor(String descriptorName, KVProvider kvProvider){
        super(descriptorName,kvProvider);
    }

    @Override
    public Deque<T> value() {
        return (Deque<T>) kvProvider.get(descriptorName);
    }

    @Override
    public void update(Deque<T> value) {
        kvProvider.put(descriptorName,value);
    }

    @Override
    public void clear() {
        kvProvider.clear();
    }

    public void addFront(T value) {
        Deque<T> doublyLinkedList = this.value();
        doublyLinkedList.addFirst(value);
    }

    public void addBack(T value) {
        Deque<T> doublyLinkedList = this.value();
        doublyLinkedList.addLast(value);
    }

    public void removeFront() {
        Deque<T> doublyLinkedList = this.value();
        doublyLinkedList.removeFirst();
    }

    public void removeBack() {
        Deque<T> doublyLinkedList = this.value();
        doublyLinkedList.removeLast();
    }

    public void remove(T value) {
        Deque<T> doublyLinkedList = this.value();
        doublyLinkedList.remove(value);
    }
}
