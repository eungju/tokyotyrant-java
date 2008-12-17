package tokyotyrant.networking;

import java.util.Iterator;
import java.util.List;

public interface NodeLocator {
	void setNodes(List<TokyoTyrantNode> nodes);	
	
	/**
	 * All nodes
	 */
	List<TokyoTyrantNode> getAll();
	
	/**
	 * Primary node for the key
	 */
	TokyoTyrantNode getPrimary(Object key);

	/**
	 * Backup nodes for the key.
	 */
	Iterator<TokyoTyrantNode> getSequence(Object key);
}
