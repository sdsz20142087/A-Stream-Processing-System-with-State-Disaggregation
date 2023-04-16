package utils;

import operators.IKeySelector;

import java.io.Serializable;

public class DefaultKeySelector implements IKeySelector, Serializable {
    @Override
    public int getKey(Object o) {
        return o.hashCode();
    }
}
