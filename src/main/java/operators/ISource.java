package operators;

public interface ISource<T>{
    boolean hasNext();
    T next();
}
