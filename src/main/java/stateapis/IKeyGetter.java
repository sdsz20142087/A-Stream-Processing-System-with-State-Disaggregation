package stateapis;

public interface IKeyGetter {
    // returns a hashed string value of the key, should be 0x000000-0x7fffffff-like.
    String getCurrentKey();
    boolean hasKeySelector();
}
