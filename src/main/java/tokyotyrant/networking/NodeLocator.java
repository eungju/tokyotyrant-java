package tokyotyrant.networking;

import java.util.Iterator;
import java.util.List;

/**
 * All nodes should have same data.
 */
public interface NodeLocator {
	void setNodes(List<ServerNode> nodes);	
	
	/**
	 * All nodes
	 */
	List<ServerNode> getAll();
	
	/**
	 * Primary node for the key
	 */
	ServerNode getPrimary();

	/**
	 * Backup nodes for the key.
	 */
	Iterator<ServerNode> getSequence();
}
