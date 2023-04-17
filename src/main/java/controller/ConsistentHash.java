package controller;

import java.util.*;

public class ConsistentHash {
    private final SortedMap<Integer, String> circle = new TreeMap<>();
    private final Map<String, List<Integer>> virtualNodes = new HashMap<>();
    private final int numberOfReplicas;

    public ConsistentHash(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        //for (String node: nodes){
        //    addNode(node);
        //}
    }

    public List<Scheduler.Triple<String, Integer, Integer>> addNode(String node) {
        List<Integer> replicas = new ArrayList<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            int hash = getHash(node + i);
            replicas.add(hash);
            circle.put(hash, node);
        }
        virtualNodes.put(node, replicas);

        List<Scheduler.Triple<String, Integer, Integer>> ranges = new ArrayList<>();
        SortedMap<Integer, String> tailMap = circle.tailMap(replicas.get(0));
        int start = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        for (Integer replica : replicas) {
            String nodeName = circle.get(replica);
            tailMap = circle.tailMap(replica);
            int end = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            if (start > end) {
                ranges.add(new Scheduler.Triple<>(nodeName, start, Integer.MAX_VALUE));
                ranges.add(new Scheduler.Triple<>(nodeName, Integer.MIN_VALUE, end));
            } else {
                ranges.add(new Scheduler.Triple<>(nodeName, start, end));
            }
            start = end;
        }
        return ranges;
    }

    public List<Scheduler.Triple<String, Integer, Integer>> removeNode(String node) {
        List<Integer> replicas = virtualNodes.get(node);
        List<Scheduler.Triple<String, Integer, Integer>> ranges = new ArrayList<>();
        if (replicas != null) {
            for (int hash : replicas) {
                circle.remove(hash);
            }
            virtualNodes.remove(node);
            // recompute ranges
            for (Map.Entry<Integer, String> entry : circle.entrySet()) {
                int hash = entry.getKey();
                String physicalNode = entry.getValue();
                List<Integer> virtualNodeHashes = virtualNodes.get(physicalNode);
                int index = virtualNodeHashes.indexOf(hash);
                int start = virtualNodeHashes.get((index - 1 + virtualNodeHashes.size()) % virtualNodeHashes.size());
                int end = virtualNodeHashes.get((index + 1) % virtualNodeHashes.size());
                if (start > end) {
                    ranges.add(new Scheduler.Triple<>(physicalNode, start, Integer.MAX_VALUE));
                    ranges.add(new Scheduler.Triple<>(physicalNode, Integer.MIN_VALUE, end));
                } else {
                    ranges.add(new Scheduler.Triple<>(physicalNode, start, end));
                }
            }
        }
        return ranges;
    }

    /*
    public void removeValue(String value) {
        List<Integer> replicas = virtualNodes.get(key);
        if (replicas != null) {
            for (int hash : replicas) {
                circle.remove(hash);
            }
            virtualNodes.remove(key);
        }
    }
    */

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

    public static int getHash(String key) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
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
