package operators;


import stateapis.ListStateAccessor;
import stateapis.MapStateAccessor;
import stateapis.ValueStateAccessor;

public interface StateDescriptorProvider {
    ValueStateAccessor getValueStateAccessor(BaseOperator op, String stateName, Object defaultValue);
    MapStateAccessor getMapStateAccessor(BaseOperator op, String stateName);

    ListStateAccessor getListStateAccessor(BaseOperator op, String stateName);
}
