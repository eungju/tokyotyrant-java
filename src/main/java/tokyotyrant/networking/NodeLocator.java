package tokyotyrant.networking;

import java.util.Collection;
import java.util.Iterator;

/**
 * All nodes should have same data.
 */
public interface NodeLocator {
	void setNodes(ServerNode[] nodes);	
	
	/**
	 * All nodes
	 */
	Collection<ServerNode> getAll();
	
	/**
	 * Nodes for the key. First node is primary. Rest are backup.
	 */
	Iterator<ServerNode> getSequence();
}
