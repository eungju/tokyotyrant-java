package tokyotyrant.networking;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * First node is the primary node. All other nodes are backup node.
 * When the primary node is down, try backup nodes in specified order.
 */
public class ActiveStandbyNodeLocator implements NodeLocator {
	private List<ServerNode> nodes;

	public void setNodes(List<ServerNode> nodes) {
		this.nodes = Collections.unmodifiableList(nodes);
	}
	
	public List<ServerNode> getAll() {
		return nodes;
	}

	public ServerNode getPrimary() {
		return nodes.get(0);
	}

	public Iterator<ServerNode> getSequence() {
		Iterator<ServerNode> i = nodes.iterator();
		i.next();
		return i;
	}
}
