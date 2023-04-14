package consistent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashing {
    private final int replicas;
    private final Map<String, List<Integer>> virtualNodes = new HashMap<>();
    private final SortedMap<String, String> circle = new TreeMap<>();

    public ConsistentHashing( int replicas, List<PhysicalNode> physicalNodes) throws NoSuchAlgorithmException {
        this.replicas = replicas;
        for (PhysicalNode physicalNode: physicalNodes){
            addPhysicalNode(physicalNode);
        }
    }

    public void addPhysicalNode(PhysicalNode physicalNode) throws NoSuchAlgorithmException {
        circle.put(physicalNode.getAddress(), physicalNode);
        for (int i = 0; i < replicas; i++) {
            String hash = getHash(physicalNode.getAddress() + "-" + i);
            VirtualNode virtualNode = new VirtualNode(physicalNode.getAddress() + "-" + i, hash, physicalNode);
            virtualNodes.put(hash, virtualNode);
            physicalNode.addVirtualNode(virtualNode);
        }
    }

    public void removePhysicalNode(PhysicalNode physicalNode) {
        circle.remove(physicalNode.getAddress());
        for (VirtualNode virtualNode : physicalNode.getVirtualNodes()) {
            virtualNodes.remove(virtualNode.getHash());
        }
    }

    public PhysicalNode getPhysicalNode(String key) throws NoSuchAlgorithmException {
        if (circle.isEmpty()) {
            return null;
        }
        String hash = getHash(key);
        SortedMap<Integer, VirtualNode> tailMap = virtualNodes.tailMap(hash);
        int virtualHash = tailMap.isEmpty() ? virtualNodes.firstKey() : tailMap.firstKey();
        VirtualNode virtualNode = virtualNodes.get(virtualHash);
        return virtualNode.getPhysicalNode();
    }

    private String getHash(String key) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] hash = md.digest(key.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
