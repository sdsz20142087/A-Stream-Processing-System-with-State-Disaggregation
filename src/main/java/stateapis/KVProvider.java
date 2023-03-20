package stateapis;

public interface KVProvider {
    BaseState get(String state);
    void put(String state, BaseState baseState);
}


