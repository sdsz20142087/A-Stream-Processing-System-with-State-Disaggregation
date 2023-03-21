package operators;

import stateapis.MapStateAccessor;
import stateapis.StateAccessor;

public interface StateDescriptorProvider {
    StateAccessor getValueStateAccessor(BaseOperator op, String stateName);
    MapStateAccessor getMapStateAccessor(BaseOperator op, String stateName);
}
