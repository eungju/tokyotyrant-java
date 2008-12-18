package tokyotyrant.networking;

import java.util.Iterator;
import java.util.List;

/**
 * All nodes should have same data.
 */
public interface NodeLocator {
	void setNodes(List<TokyoTyrantNode> nodes);	
	
	/**
	 * All nodes
	 */
	List<TokyoTyrantNode> getAll();
	
	/**
	 * Primary node for the key
	 */
	TokyoTyrantNode getPrimary();

	/**
	 * Backup nodes for the key.
	 */
	Iterator<TokyoTyrantNode> getSequence();
}
