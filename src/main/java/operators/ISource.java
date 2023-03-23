package operators;

import java.io.IOException;

public interface ISource<T> {
    void init() throws IOException;
    boolean hasNext();
    T next();
}
