package operators;

import java.io.Serializable;

public interface IKeySelector extends Serializable {
    String getUniqueKey(Object o);
}
