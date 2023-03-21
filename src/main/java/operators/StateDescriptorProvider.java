package operators;

import stateapis.ValueState;
import stateapis.MapStateAccessor;

public interface StateDescriptorProvider {
    ValueState getValueStateAccessor(BaseOperator op, String stateName);
    MapStateAccessor getMapStateAccessor(BaseOperator op, String stateName);

    ListStateAccessor getListStateAccessor(BaseOperator op, String stateName);
}
