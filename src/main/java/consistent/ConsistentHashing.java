package consistent;

import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashing {
    private final int numberOfReplicas;
    private final Map<PhysicalNode, List<VirtualNode>> virtualNodes = new HashMap<>();
    private final SortedMap<Integer, PhysicalNode> circle = new TreeMap<>();

    public ConsistentHashing( int replicas){
        this.numberOfReplicas = replicas;
    }

    public void add(PhysicalNode physicalNode) throws NoSuchAlgorithmException {
        List<VirtualNode> replicas = new ArrayList<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            int hash = getHash(physicalNode.getKey() + "-" + i);
            VirtualNode virtualNode = new VirtualNode(physicalNode.getKey() + "-" + i, hash, physicalNode);
            physicalNode.addVirtualNode(virtualNode);
            replicas.add(virtualNode);
            circle.put(hash, physicalNode);
        }
        virtualNodes.put(physicalNode, replicas);
    }

    public void remove(PhysicalNode physicalNode) {
        circle.remove(getHash(physicalNode.getKey()));
        for (VirtualNode virtualNode : physicalNode.getVirtualNodes()) {
            virtualNodes.remove(virtualNode.getHash());
        }
    }

    public PhysicalNode get(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = getHash(key);
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, PhysicalNode> tailMap = circle.tailMap(hash);
            int virtualHash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            VirtualNode virtualNode = virtualNodes.get(virtualHash).get(0); // get the first virtual node for the selected physical node
            return virtualNode.getPhysicalNode();
        }
        return circle.get(hash);
    }

    private int getHash(String key){
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
