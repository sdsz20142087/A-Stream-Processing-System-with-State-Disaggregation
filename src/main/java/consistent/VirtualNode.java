package consistent;

public class VirtualNode {
    private String address;
    private int hash;
    private PhysicalNode physicalNode;

    public VirtualNode(String address, int hash, PhysicalNode physicalNode) {
        this.address = address;
        this.hash = hash;
        this.physicalNode = physicalNode;
    }

    public String getAddress() {
        return address;
    }

    public int getHash() {
        return hash;
    }

    public PhysicalNode getPhysicalNode() {
        return physicalNode;
    }

}
