package tokyotyrant.networking;

import java.util.Iterator;

public class NodeSelector {
	private NodeLocator nodeLocator;
	
	public NodeSelector(NodeLocator nodeLocator) {
		this.nodeLocator = nodeLocator;
	}

	public ServerNode select() {
		Iterator<ServerNode> backups = nodeLocator.getSequence();
		ServerNode selected = backups.next();
		if (selected.isActive()) {
			return selected;
		}
		while (backups.hasNext()) {
			ServerNode each = backups.next();
			if (each.isActive()) {
				selected = each;
				break;
			}
		}
		return selected;
	}
}
