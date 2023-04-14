package consistent;

import java.util.ArrayList;
import java.util.List;

public class PhysicalNode {
    private String key;
    private List<VirtualNode> virtualNodes;

    public PhysicalNode(String key) {
        this.key = key;
        this.virtualNodes = new ArrayList<>();
    }

    public String getKey() {
        return key;
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
