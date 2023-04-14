package consistent;

public class VirtualNode {
    private String key;
    private int hash;
    private PhysicalNode physicalNode;

    public VirtualNode(String key, int hash, PhysicalNode physicalNode) {
        this.key = key;
        this.hash = hash;
        this.physicalNode = physicalNode;
    }

    public String getKey() {
        return key;
    }

    public int getHash() {
        return hash;
    }

    public PhysicalNode getPhysicalNode() {
        return physicalNode;
    }

}
