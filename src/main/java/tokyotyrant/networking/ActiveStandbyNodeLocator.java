package tokyotyrant.networking;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * First node is the primary node. All other nodes are backup node.
 * When the primary node is down, try backup nodes in specified order.
 */
public class ActiveStandbyNodeLocator implements NodeLocator {
	private List<TokyoTyrantNode> nodes;

	public void setNodes(List<TokyoTyrantNode> nodes) {
		this.nodes = Collections.unmodifiableList(nodes);
	}
	
	public List<TokyoTyrantNode> getAll() {
		return nodes;
	}

	public TokyoTyrantNode getPrimary() {
		return nodes.get(0);
	}

	public Iterator<TokyoTyrantNode> getSequence() {
		Iterator<TokyoTyrantNode> i = nodes.iterator();
		i.next();
		return i;
	}
}
