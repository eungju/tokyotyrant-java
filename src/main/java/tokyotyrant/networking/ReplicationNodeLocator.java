package tokyotyrant.networking;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ReplicationNodeLocator implements NodeLocator {
	private List<TokyoTyrantNode> nodes;

	public void setNodes(List<TokyoTyrantNode> nodes) {
		this.nodes = Collections.unmodifiableList(nodes);
	}
	
	public List<TokyoTyrantNode> getAll() {
		return nodes;
	}

	public TokyoTyrantNode getPrimary(Object key) {
		return nodes.get(0);
	}

	public Iterator<TokyoTyrantNode> getSequence(Object key) {
		Iterator<TokyoTyrantNode> i = nodes.iterator();
		i.next();
		return i;
	}
}
