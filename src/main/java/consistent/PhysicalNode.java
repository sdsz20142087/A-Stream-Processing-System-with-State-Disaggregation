package consistent;

import java.util.ArrayList;
import java.util.List;

public class PhysicalNode {
    private String address;
    private List<VirtualNode> virtualNodes;

    public PhysicalNode(String address) {
        this.address = address;
        this.virtualNodes = new ArrayList<>();
    }

    public String getAddress() {
        return address;
    }

    public List<VirtualNode> getVirtualNodes() {
        return virtualNodes;
    }

    public void addVirtualNode(VirtualNode virtualNode) {
        virtualNodes.add(virtualNode);
    }

    public void removeVirtualNode(VirtualNode virtualNode) {
        virtualNodes.remove(virtualNode);
    }


}
