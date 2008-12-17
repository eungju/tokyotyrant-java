package tokyotyrant.networking;

import tokyotyrant.protocol.Command;

public abstract class AbstractNetworking implements Networking {
	protected NodeLocator nodeLocator;
	protected ReconnectQueue reconnectQueue;
	
	protected AbstractNetworking(NodeLocator nodeLocator) {
		this.nodeLocator = nodeLocator;
		this.reconnectQueue = new ReconnectQueue();
	}
	
	public void send(Command<?> command) {
		//FIXME: Use real key
		selectNode("FAKE_KEY").send(command);
	}
	
	protected TokyoTyrantNode selectNode(Object key) {
		TokyoTyrantNode selected = nodeLocator.getPrimary(key);
		if (selected.isActive()) {
			return selected;
		}
		for (TokyoTyrantNode each : nodeLocator.getAll()) {
			if (each.isActive()) {
				selected = each;
				break;
			}
		}
		return selected;
	}
	
	protected void connectAllNodes() {
		for (TokyoTyrantNode each : nodeLocator.getAll()) {
			if (!each.connect()) {
				reconnectQueue.push(each);
			}
		}
	}
	
	protected void disconnectAllNodes() {
		for (TokyoTyrantNode each : nodeLocator.getAll()) {
			each.disconnect();
		}
	}
}
