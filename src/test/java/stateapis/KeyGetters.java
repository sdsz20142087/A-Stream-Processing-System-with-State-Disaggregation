package stateapis;


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
    @Override
    public String getCurrentKey() {
        return "0x0000ffff";
    }

    @Override
    public boolean hasKeySelector() {
        return true;
    }
}