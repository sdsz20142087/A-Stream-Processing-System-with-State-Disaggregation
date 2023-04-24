package stateapis;


import operators.IKeySelector;
import utils.KeyUtil;

class NullKeyGetter implements IKeyGetter {
    @Override
    public String getCurrentKey() {
        return null;
    }

    @Override
    public boolean hasKeySelector() {
        return false;
    }
}

class ValidKeyGetter implements IKeyGetter {

    public ValidKeyGetter() {
        currentObj = new String("test");
    }

    private Object currentObj;

    public void setObj(Object o) {
        currentObj = o;
    }

    @Override
    public String getCurrentKey() {
        Object o = currentObj;
        String hashed = KeyUtil.objToKey(o, new IKeySelector() {
            @Override
            public String getUniqueKey(Object o) {
                return o.toString();
            }
        });
        return hashed;
    }

    @Override
    public boolean hasKeySelector() {
        return true;
    }
}