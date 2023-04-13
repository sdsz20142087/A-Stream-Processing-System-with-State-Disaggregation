package operators;

import java.io.IOException;

public interface ISource<T> {
    void init() throws IOException;
    T next();
}
