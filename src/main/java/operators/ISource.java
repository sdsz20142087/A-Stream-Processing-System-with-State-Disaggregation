package operators;

public interface ISource<T> {
    void init() throws Exception;
    boolean hasNext();
    T next();
}
