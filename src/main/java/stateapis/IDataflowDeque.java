package stateapis;

public interface IDataflowDeque<T> {
    void addFirst(T value);
    void addLast(T value);
    T removeFirst();
    T removeLast();
    T peekFirst();
    T peekLast();
    void clear();
    boolean isEmpty();
    int size();
}
