package controller;

import java.util.*;

public class ConsistentHash {
    private final SortedMap<Integer, String> circle = new TreeMap<>();
    private final Map<String, List<Integer>> virtualNodes = new HashMap<>();
    private final int numberOfReplicas;

    public ConsistentHash(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    public void add(String key) {
        List<Integer> replicas = new ArrayList<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            int hash = getHash(key + i);
            replicas.add(hash);
            circle.put(hash, key);
        }
        virtualNodes.put(key, replicas);
    }

    public void remove(String key) {
        List<Integer> replicas = virtualNodes.get(key);
        if (replicas != null) {
            for (int hash : replicas) {
                circle.remove(hash);
            }
            virtualNodes.remove(key);
        }
    }

    public String get(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = getHash(key);
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private int getHash(String key) {
        final int p = 16777619;
        int hash = (int)2166136261L;
        for (int i = 0; i < key.length(); i++) {
            hash = (hash ^ key.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return hash;
    }
}
