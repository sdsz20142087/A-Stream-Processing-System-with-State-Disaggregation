package operators;

import java.io.Serializable;

public interface IKeySelector extends Serializable {
    int getKey(Object o);
}
