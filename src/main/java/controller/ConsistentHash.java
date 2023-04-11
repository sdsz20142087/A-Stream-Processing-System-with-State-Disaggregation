package controller;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class ConsistentHash {
    private final int numReplicas;
    private final TreeMap<String, String> circle = new TreeMap<>();

    public ConsistentHash(int numReplicas) {
        this.numReplicas = numReplicas;
    }

    public void add(String key, String address) {
        for (int i = 0; i < numReplicas; i++) {
            //String virtualAddress = address + "-" + i;
            //String hash = hash(virtualAddress);
            String hash = hash(key);
            circle.put(hash, address);
        }
    }

    public void remove(String key) {
        for (int i = 0; i < numReplicas; i++) {
            //String virtualAddress = address + "-" + i;
            //String hash = hash(virtualAddress);
            String hash = hash(key);
            circle.remove(hash);
        }
    }

    public String get(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        String hash = hash(key);
        Map.Entry<String, String> entry = circle.ceilingEntry(hash);
        if (entry == null) {
            entry = circle.firstEntry();
        }
        return entry.getValue();
    }

    private String hash(String key) {
        return Integer.toString(Objects.hash(key));
    }
}
